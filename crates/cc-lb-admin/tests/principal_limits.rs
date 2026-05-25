use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use cc_lb_admin::{AdminState, router};
use cc_lb_config::{Config, PrincipalSpec};
use cc_lb_core::DashboardBroadcaster;
use cc_lb_storage_redb::{
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, RedbStorage,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn existing_principal_limits_group_and_order_identities_windows_and_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("principal-limits.redb");
    let storage = RedbStorage::open(&path).unwrap();
    seed_ordered_limit_states(&storage, "principal-a");

    let app = router(test_state(
        config_with_principals(&["principal-a"]),
        Some(Arc::new(storage)),
    ));
    let (status, json, _) = authorized_json(app, "/admin/principals/principal-a/limits").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["principal_id"], "principal-a");
    assert_eq!(json["observed"], true);

    let identities = json["identities"].as_array().unwrap();
    assert_eq!(identities.len(), 3);
    assert_eq!(
        identity_kinds(identities),
        vec!["account", "credential", "unobserved"]
    );
    assert_eq!(identities[0]["account_observed"], true);
    assert_eq!(identities[1]["account_observed"], false);
    assert_eq!(identities[2]["account_observed"], false);

    let account_windows = identities[0]["windows"].as_array().unwrap();
    assert_eq!(window_names(account_windows), vec!["5h", "weekly"]);
    assert_eq!(
        snapshot_kinds(account_windows[0]["snapshots"].as_array().unwrap()),
        vec!["requests", "input_tokens", "output_tokens", "tokens"]
    );
    assert_eq!(
        snapshot_kinds(account_windows[1]["snapshots"].as_array().unwrap()),
        vec!["input_tokens"]
    );

    let credential_windows = identities[1]["windows"].as_array().unwrap();
    assert_eq!(window_names(credential_windows), vec!["5h"]);
    assert_eq!(
        snapshot_kinds(credential_windows[0]["snapshots"].as_array().unwrap()),
        vec!["tokens"]
    );
    let unobserved_snapshot = &identities[2]["windows"][0]["snapshots"][0];
    assert_eq!(unobserved_snapshot["observed"], false);
}

#[tokio::test]
async fn account_observed_is_false_without_account_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("principal-limits.redb");
    let storage = RedbStorage::open(&path).unwrap();
    storage
        .put_principal_limit_state(&limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Credential,
            Some("credential-1"),
            "5h",
            PrincipalLimitKind::Requests,
            Some(100),
            Some(90),
            10,
        ))
        .unwrap();
    storage
        .put_principal_limit_state(&limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Unobserved,
            None,
            "weekly",
            PrincipalLimitKind::Tokens,
            Some(200),
            Some(180),
            20,
        ))
        .unwrap();

    let app = router(test_state(
        config_with_principals(&["principal-a"]),
        Some(Arc::new(storage)),
    ));
    let (status, json, _) = authorized_json(app, "/admin/principals/principal-a/limits").await;

    assert_eq!(status, StatusCode::OK);
    let identities = json["identities"].as_array().unwrap();
    assert_eq!(identity_kinds(identities), vec!["credential", "unobserved"]);
    assert!(
        identities
            .iter()
            .all(|identity| identity["account_observed"] == false)
    );
}

#[tokio::test]
async fn five_hour_and_weekly_limits_json_shape_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("principal-limits.redb");
    let storage = RedbStorage::open(&path).unwrap();
    storage
        .put_principal_limit_state(&limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "5h",
            PrincipalLimitKind::Requests,
            Some(5000),
            Some(4999),
            1,
        ))
        .unwrap();
    storage
        .put_principal_limit_state(&limit_state(
            "principal-a",
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "weekly",
            PrincipalLimitKind::InputTokens,
            Some(100000),
            Some(99999),
            2,
        ))
        .unwrap();

    let app = router(test_state(
        config_with_principals(&["principal-a"]),
        Some(Arc::new(storage)),
    ));
    let (status, actual, _) = authorized_json(app, "/admin/principals/principal-a/limits").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        actual,
        json!({
            "principal_id": "principal-a",
            "observed": true,
            "identities": [
                {
                    "identity_kind": "account",
                    "identity_value": "acc-1",
                    "account_observed": true,
                    "windows": [
                        {
                            "window": "5h",
                            "snapshots": [
                                {
                                    "kind": "requests",
                                    "limit": 5000,
                                    "remaining": 4999,
                                    "reset": "2026-05-20T00:00:01Z",
                                    "observed_at_unix_secs": 1_764_000_001_u64,
                                    "stored_at_unix_secs": 1_764_000_101_u64,
                                    "observed": true
                                }
                            ]
                        },
                        {
                            "window": "weekly",
                            "snapshots": [
                                {
                                    "kind": "input_tokens",
                                    "limit": 100000,
                                    "remaining": 99999,
                                    "reset": "2026-05-20T00:00:02Z",
                                    "observed_at_unix_secs": 1_764_000_002_u64,
                                    "stored_at_unix_secs": 1_764_000_102_u64,
                                    "observed": true
                                }
                            ]
                        }
                    ]
                }
            ]
        })
    );
}

#[tokio::test]
async fn unknown_principal_limits_returns_404() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, json, _) = authorized_json(app, "/admin/principals/ghost/limits").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json, json!({ "error": "unknown_principal" }));
}

#[tokio::test]
async fn empty_storage_limits_returns_empty_unobserved_identities() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, json, _) = authorized_json(app, "/admin/principals/principal-a/limits").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["observed"], false);
    assert_eq!(json["identities"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn principal_limits_requires_admin_auth() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/principals/principal-a/limits")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn principal_limits_response_excludes_payload_terms() {
    let app = router(test_state(config_with_principals(&["principal-a"]), None));
    let (status, _, body) = authorized_json(app, "/admin/principals/principal-a/limits").await;

    assert_eq!(status, StatusCode::OK);
    assert_forbidden_bytes_absent(&body);
}

fn seed_ordered_limit_states(storage: &RedbStorage, principal_id: &str) {
    for state in [
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Credential,
            Some("credential-1"),
            "5h",
            PrincipalLimitKind::Tokens,
            Some(1000),
            Some(900),
            5,
        ),
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "weekly",
            PrincipalLimitKind::InputTokens,
            Some(100000),
            Some(99990),
            4,
        ),
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Unobserved,
            None,
            "5h",
            PrincipalLimitKind::OutputTokens,
            None,
            None,
            6,
        ),
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "5h",
            PrincipalLimitKind::Tokens,
            Some(4000),
            Some(3900),
            3,
        ),
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "5h",
            PrincipalLimitKind::OutputTokens,
            Some(3000),
            Some(2900),
            2,
        ),
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "5h",
            PrincipalLimitKind::Requests,
            Some(2000),
            Some(1900),
            1,
        ),
        limit_state(
            principal_id,
            PrincipalLimitIdentityKind::Account,
            Some("acc-1"),
            "5h",
            PrincipalLimitKind::InputTokens,
            Some(2500),
            Some(2400),
            7,
        ),
    ] {
        storage.put_principal_limit_state(&state).unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
fn limit_state(
    principal_id: &str,
    identity_kind: PrincipalLimitIdentityKind,
    identity_value: Option<&str>,
    window: &str,
    kind: PrincipalLimitKind,
    limit: Option<u64>,
    remaining: Option<u64>,
    offset: u64,
) -> PrincipalLimitState {
    PrincipalLimitState {
        principal_id: principal_id.to_owned(),
        identity_kind,
        identity_value: identity_value.map(ToOwned::to_owned),
        account_observed: identity_kind == PrincipalLimitIdentityKind::Account,
        window: window.to_owned(),
        kind,
        limit,
        remaining,
        reset: Some(format!("2026-05-20T00:00:{offset:02}Z")),
        observed_at_unix_secs: 1_764_000_000 + offset,
        stored_at_unix_secs: 1_764_000_100 + offset,
    }
}

fn test_state(config: Config, storage: Option<Arc<RedbStorage>>) -> AdminState {
    AdminState {
        storage: storage.unwrap_or_else(test_storage),
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
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

fn config_with_principals(ids: &[&str]) -> Config {
    let mut config = Config::default();
    for id in ids {
        config
            .principals
            .insert((*id).to_owned(), PrincipalSpec::default());
    }
    config
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

fn identity_kinds(identities: &[Value]) -> Vec<&str> {
    identities
        .iter()
        .map(|identity| identity["identity_kind"].as_str().unwrap())
        .collect()
}

fn window_names(windows: &[Value]) -> Vec<&str> {
    windows
        .iter()
        .map(|window| window["window"].as_str().unwrap())
        .collect()
}

fn snapshot_kinds(snapshots: &[Value]) -> Vec<&str> {
    snapshots
        .iter()
        .map(|snapshot| snapshot["kind"].as_str().unwrap())
        .collect()
}

fn assert_forbidden_bytes_absent(body: &[u8]) {
    for forbidden in forbidden_terms() {
        assert!(
            !body
                .windows(forbidden.len())
                .any(|window| window == forbidden.as_bytes()),
            "forbidden term present: {forbidden}"
        );
    }
}

fn forbidden_terms() -> Vec<String> {
    vec![
        ["mes", "sages"].concat(),
        ["sys", "tem"].concat(),
        ["too", "ls"].concat(),
        ["tool", "_use"].concat(),
        ["con", "tent"].concat(),
    ]
}

fn test_storage() -> Arc<RedbStorage> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.redb");
    let storage = Arc::new(RedbStorage::open(&path).unwrap());
    std::mem::forget(dir);
    storage
}
