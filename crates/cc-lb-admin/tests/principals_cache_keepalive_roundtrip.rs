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
async fn cache_keepalive_survives_create_get_and_put_clear() {
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
async fn cache_keepalive_omitted_on_update_remains_unchanged() {
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
async fn cache_keepalive_rejects_unimplemented_llm_judge_on_create() {
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
async fn cache_keepalive_rejects_unimplemented_llm_judge_on_update() {
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

#[tokio::test]
async fn prompt_cache_mode_defaults_round_trips_and_survives_unrelated_updates() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (created, etag) = create_principal(
        state.clone(),
        json!({
            "name": "cache-mode-roundtrip",
            "kind": "machine",
            "cache_keepalive": { "enabled": true }
        }),
    )
    .await;
    let id = created["id"].as_str().expect("principal id").to_owned();

    assert_eq!(
        created["prompt_cache_mode"], "preserve_client",
        "create omission defaults to preserve_client"
    );

    let (patch_status, patch_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({ "prompt_cache_mode": "one_hour_no_keepalive" }),
    )
    .await;
    assert_eq!(patch_status, StatusCode::OK, "patch failed: {patch_body:?}");
    assert_eq!(patch_body["prompt_cache_mode"], "one_hour_no_keepalive");
    assert_eq!(
        patch_body["cache_keepalive"]["enabled"],
        Value::Bool(true),
        "the stored keep-alive configuration stays, dormant"
    );

    // An unrelated update keeps the mode, and a keep-alive-only update cannot
    // erase it either.
    let revision = patch_body["revision"].as_u64().expect("revision");
    let (keepalive_status, keepalive_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &format!("W/\"{revision}\""),
        json!({ "cache_keepalive": { "enabled": false } }),
    )
    .await;
    assert_eq!(
        keepalive_status,
        StatusCode::OK,
        "update failed: {keepalive_body:?}"
    );
    assert_eq!(
        keepalive_body["prompt_cache_mode"], "one_hour_no_keepalive",
        "omitting the mode preserves it"
    );
    assert_eq!(
        keepalive_body["cache_keepalive"]["enabled"],
        Value::Bool(false)
    );

    let (list_status, _list_headers, list_body, _raw) =
        authed_json(app(state.clone()), "GET", "/admin/v1/principals", None).await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(
        list_body["principals"][0]["prompt_cache_mode"], "one_hour_no_keepalive",
        "list responses carry the stored mode"
    );

    // Explicitly resetting restores client policy without touching keep-alives.
    let revision = keepalive_body["revision"].as_u64().expect("revision");
    let (reset_status, reset_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &format!("W/\"{revision}\""),
        json!({ "prompt_cache_mode": "preserve_client" }),
    )
    .await;
    assert_eq!(reset_status, StatusCode::OK, "reset failed: {reset_body:?}");
    assert_eq!(reset_body["prompt_cache_mode"], "preserve_client");

    let (get_status, _get_headers, get_body, _raw) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_body["prompt_cache_mode"], "preserve_client");
}

#[tokio::test]
async fn prompt_cache_mode_rejects_unknown_values_and_null() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (created, etag) = create_principal(
        state.clone(),
        json!({ "name": "cache-mode-invalid", "kind": "machine" }),
    )
    .await;
    let id = created["id"].as_str().expect("principal id").to_owned();

    for body in [
        json!({ "prompt_cache_mode": "one_hour" }),
        json!({ "prompt_cache_mode": null }),
        json!({ "prompt_cache_mode": "ONE_HOUR_NO_KEEPALIVE" }),
    ] {
        let (status, response) = authed_put_json(
            app(state.clone()),
            &format!("/admin/v1/principals/{id}"),
            &etag,
            body.clone(),
        )
        .await;
        assert!(
            !status.is_success(),
            "invalid mode {body:?} must be rejected, got {status} {response:?}"
        );
    }

    let (get_status, _headers, get_body, _raw) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(
        get_body["prompt_cache_mode"], "preserve_client",
        "a rejected write leaves the stored mode alone"
    );
}

#[tokio::test]
async fn prompt_cache_mode_requires_a_current_revision() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (created, etag) = create_principal(
        state.clone(),
        json!({ "name": "cache-mode-stale", "kind": "machine" }),
    )
    .await;
    let id = created["id"].as_str().expect("principal id").to_owned();

    let (first_status, first_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({ "prompt_cache_mode": "one_hour_no_keepalive" }),
    )
    .await;
    assert_eq!(first_status, StatusCode::OK, "patch failed: {first_body:?}");

    // The same If-Match value is stale now: the mode write must conflict and
    // leave the stored mode untouched.
    let (stale_status, stale_body) = authed_put_json(
        app(state.clone()),
        &format!("/admin/v1/principals/{id}"),
        &etag,
        json!({ "prompt_cache_mode": "preserve_client" }),
    )
    .await;
    assert_eq!(stale_status, StatusCode::CONFLICT, "body: {stale_body:?}");

    let (get_status, _headers, get_body, _raw) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_body["prompt_cache_mode"], "one_hour_no_keepalive");
}
