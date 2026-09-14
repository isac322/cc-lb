use crate::config_admin_common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_admin::AdminState;
use cc_lb_config::Config;
use config_admin_common::{TOKEN, app, authed_json, temp_storage, test_state};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn authed_put_json(
    app: axum::Router,
    uri: &str,
    if_match: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .header("If-Match", if_match)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let raw = response.into_body().collect().await.unwrap().to_bytes();
    let value = if raw.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&raw).unwrap_or(Value::Null)
    };
    (status, value)
}

async fn create_principal(state: AdminState, body: Value) -> (Value, String) {
    let (status, headers, body, _raw) =
        authed_json(app(state), "POST", "/admin/v1/principals", Some(body)).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "principal create failed: {body:?}"
    );
    let etag = headers
        .get("etag")
        .expect("etag on create")
        .to_str()
        .expect("etag string")
        .to_owned();
    (body, etag)
}

#[tokio::test]
async fn t2__cache_keepalive_survives_create_get_and_put_clear() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let create_body = json!({
        "name": "kp-roundtrip",
        "kind": "machine",
        "cache_keepalive": {
            "enabled": true,
            "refresh_lead_time_5m_secs": 42,
        }
    });
    let (created, etag) = create_principal(state.clone(), create_body).await;

    let id = created["id"].as_str().expect("principal id").to_owned();

    let (get_status, _get_headers, get_body, _raw) = authed_json(
        app(state.clone()),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_body["cache_keepalive"]["enabled"], Value::Bool(true));
    assert_eq!(
        get_body["cache_keepalive"]["refresh_lead_time_5m_secs"],
        Value::from(42)
    );

    let (put_status, put_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({ "cache_keepalive": null }),
    )
    .await;
    assert_eq!(put_status, StatusCode::OK, "put failed: {put_body:?}");

    let (get2_status, _, get2_body, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(get2_status, StatusCode::OK);
    assert!(
        get2_body["cache_keepalive"].is_null(),
        "cache_keepalive should be null after clear, got {:?}",
        get2_body["cache_keepalive"]
    );
}

#[tokio::test]
async fn t2__cache_keepalive_omitted_on_update_remains_unchanged() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (created, etag) = create_principal(
        state.clone(),
        json!({
            "name": "kp-preserve",
            "kind": "machine",
            "cache_keepalive": {
                "enabled": true,
                "refresh_lead_time_5m_secs": 42,
                "max_refreshes_per_session": 5
            }
        }),
    )
    .await;
    let id = created["id"].as_str().expect("principal id").to_owned();

    let (put_status, put_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({ "name": "kp-preserve-renamed" }),
    )
    .await;

    assert_eq!(put_status, StatusCode::OK, "put failed: {put_body:?}");
    assert_eq!(put_body["name"], "kp-preserve-renamed");
    assert_eq!(put_body["cache_keepalive"]["enabled"], Value::Bool(true));
    assert_eq!(put_body["cache_keepalive"]["refresh_lead_time_5m_secs"], 42);
    assert_eq!(put_body["cache_keepalive"]["max_refreshes_per_session"], 5);
}

#[tokio::test]
async fn t2__cache_keepalive_rejects_unimplemented_llm_judge_on_create() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _headers, body, _raw) = authed_json(
        app(state),
        "POST",
        "/admin/v1/principals",
        Some(json!({
            "name": "kp-judge-create",
            "kind": "machine",
            "cache_keepalive": {
                "enabled": true,
                "classifier": {
                    "llm_judge": {
                        "provider": "anthropic",
                        "model": "claude-haiku-4-5",
                        "api_key_secret_ref": "secret://judge"
                    }
                }
            }
        })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body={body:?}");
    assert_eq!(body["error"], "unsupported_cache_keepalive_llm_judge");
    assert_eq!(body["field"], "cache_keepalive.classifier.llm_judge");
}

#[tokio::test]
async fn t2__cache_keepalive_rejects_unimplemented_llm_judge_on_update() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (created, etag) = create_principal(
        state.clone(),
        json!({
            "name": "kp-judge-update",
            "kind": "machine",
            "cache_keepalive": { "enabled": true }
        }),
    )
    .await;
    let id = created["id"].as_str().expect("principal id").to_owned();

    let (status, body) = authed_put_json(
        app(state),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({
            "cache_keepalive": {
                "enabled": true,
                "classifier": {
                    "llm_judge": {
                        "provider": "anthropic",
                        "model": "claude-haiku-4-5",
                        "api_key_secret_ref": "secret://judge"
                    }
                }
            }
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "body={body:?}");
    assert_eq!(body["error"], "unsupported_cache_keepalive_llm_judge");
    assert_eq!(body["field"], "cache_keepalive.classifier.llm_judge");
}
