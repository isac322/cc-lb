mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::{Config, PrincipalSpec};
use cc_lb_storage_redb::OAuthCredentials;
use config_admin_common::{
    app, assert_private, authed_bytes, authed_json, temp_storage, test_state,
    test_state_without_storage, unauthenticated_status,
};
use serde_json::json;

const OAUTH_PROVIDER: &str = "anthropic_oauth";
const API_KEY_PROVIDER: &str = "api_key";
const LEAK_MARKER: &str = "LEAK-TOKEN-DO-NOT-RETURN";

#[tokio::test]
async fn list_credentials_without_storage_is_unobserved() {
    let app = app(test_state_without_storage());

    let (status, _, body, _) = authed_json(app, "GET", "/admin/credentials", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["observed"], false);
    assert_eq!(body["credentials"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_credentials_reports_oauth_and_api_key_kinds() {
    let (_dir, storage) = temp_storage();
    storage
        .put_oauth("alice", OAUTH_PROVIDER, &oauth_credentials(3_600))
        .unwrap();
    storage
        .issue_api_key("bob", Some("bob-key".to_owned()))
        .unwrap();
    let app = app(test_state(credentials_config(), Some(storage)));

    let (status, _, body, _) = authed_json(app, "GET", "/admin/credentials", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["observed"], true);
    let credentials = body["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 2);
    let alice = credential_for(credentials, "alice", OAUTH_PROVIDER);
    assert_eq!(alice["kind"], "oauth");
    assert_eq!(alice["has_credentials"], true);
    assert_eq!(alice["status"], "valid");
    let bob = credential_for(credentials, "bob", API_KEY_PROVIDER);
    assert_eq!(bob["kind"], "api_key");
    assert_eq!(bob["has_credentials"], true);
    assert_eq!(bob["status"], "valid");
}

#[tokio::test]
async fn revoke_api_key_credential_revokes_all_active_keys() {
    let (_dir, storage) = temp_storage();
    let first = storage
        .issue_api_key("bob", Some("first".to_owned()))
        .unwrap();
    let second = storage
        .issue_api_key("bob", Some("second".to_owned()))
        .unwrap();
    let app = app(test_state(credentials_config(), Some(storage.clone())));

    let (status, _, body, _) =
        authed_json(app, "POST", "/admin/credentials/bob/api_key/revoke", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"], "api_key");
    assert!(body["revoked_keys"]
        .as_array()
        .unwrap()
        .contains(&json!(first.key_id)));
    assert!(body["revoked_keys"]
        .as_array()
        .unwrap()
        .contains(&json!(second.key_id)));
    let keys = storage.list_api_keys("bob").unwrap();
    assert!(keys
        .iter()
        .all(|record| record.revoked_at_unix_secs.is_some()));
}

#[tokio::test]
async fn rotate_api_key_credential_returns_new_plaintext_once_and_revokes_old_key() {
    let (_dir, storage) = temp_storage();
    let old = storage
        .issue_api_key("bob", Some("old".to_owned()))
        .unwrap();
    let app = app(test_state(credentials_config(), Some(storage.clone())));

    let (status, _, body, body_bytes) =
        authed_json(app, "POST", "/admin/credentials/bob/api_key/rotate", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"], "api_key");
    assert_eq!(body["revoked_key_id"], json!(old.key_id.clone()));
    let new_key_id = body["new_key_id"].as_str().unwrap();
    let plaintext = body["plaintext_key"].as_str().unwrap();
    assert_eq!(new_key_id.len(), 12);
    assert_eq!(plaintext.len(), 43);
    assert_eq!(count_occurrences(&body_bytes, plaintext.as_bytes()), 1);

    let keys = storage.list_api_keys("bob").unwrap();
    let old_record = keys
        .iter()
        .find(|record| record.key_id == old.key_id)
        .unwrap();
    let new_record = keys
        .iter()
        .find(|record| record.key_id == new_key_id)
        .unwrap();
    assert!(old_record.revoked_at_unix_secs.is_some());
    assert!(new_record.revoked_at_unix_secs.is_none());
}

#[tokio::test]
async fn rotate_oauth_credential_is_unsupported() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(credentials_config(), Some(storage)));

    let (status, _, body, _) = authed_json(
        app,
        "POST",
        "/admin/credentials/alice/anthropic_oauth/rotate",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body["error"], "rotate_unsupported");
    assert_eq!(body["kind"], "oauth");
}

#[tokio::test]
async fn credentials_list_requires_admin_auth() {
    let app = app(test_state_without_storage());

    let status = unauthenticated_status(app, "GET", "/admin/credentials").await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn credential_responses_never_return_oauth_secret_material() {
    let (_dir, storage) = temp_storage();
    storage
        .put_oauth(
            "alice",
            OAUTH_PROVIDER,
            &OAuthCredentials {
                access_token: LEAK_MARKER.to_owned(),
                refresh_token: LEAK_MARKER.to_owned(),
                expires_at: unix_now_secs() + 3_600,
                scopes: vec![LEAK_MARKER.to_owned()],
            },
        )
        .unwrap();
    let app = app(test_state(credentials_config(), Some(storage)));

    let (_, _, list_body) = authed_bytes(app.clone(), "GET", "/admin/credentials", None).await;
    let (_, _, rotate_body) = authed_bytes(
        app.clone(),
        "POST",
        "/admin/credentials/alice/anthropic_oauth/rotate",
        None,
    )
    .await;
    let (_, _, revoke_body) = authed_bytes(
        app,
        "POST",
        "/admin/credentials/alice/anthropic_oauth/revoke",
        None,
    )
    .await;

    for body in [&list_body, &rotate_body, &revoke_body] {
        assert_private(body);
        assert!(!contains_bytes(body, LEAK_MARKER.as_bytes()));
    }
}

fn credentials_config() -> Config {
    let mut config = Config::default();
    config.principals.insert(
        "alice".to_owned(),
        PrincipalSpec {
            credentials_ref: Some(OAUTH_PROVIDER.to_owned()),
            ..PrincipalSpec::default()
        },
    );
    config.principals.insert(
        "bob".to_owned(),
        PrincipalSpec {
            allowed_models: Vec::new(),
            ..PrincipalSpec::default()
        },
    );
    config
}

fn credential_for<'a>(
    credentials: &'a [serde_json::Value],
    principal_id: &str,
    provider: &str,
) -> &'a serde_json::Value {
    credentials
        .iter()
        .find(|credential| {
            credential["principal_id"] == principal_id && credential["provider"] == provider
        })
        .unwrap()
}

fn oauth_credentials(expires_in_secs: u64) -> OAuthCredentials {
    OAuthCredentials {
        access_token: "access-token".to_owned(),
        refresh_token: "refresh-token".to_owned(),
        expires_at: unix_now_secs() + expires_in_secs,
        scopes: Vec::new(),
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}
