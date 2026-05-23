mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::{Config, PrincipalSpec};
use config_admin_common::{
    app, assert_private, authed_bytes, authed_json, temp_storage, test_state,
    unauthenticated_status,
};
use serde_json::{json, Value};

#[tokio::test]
async fn issue_list_and_revoke_key_preserves_one_time_plaintext_contract() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(
        config_with_principal("alice"),
        Some(storage.clone()),
    ));

    let (status, _, issue, issue_body) = authed_json(
        app.clone(),
        "POST",
        "/admin/principals/alice/keys",
        Some(json!({ "label": "test-key" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let plaintext = issue["plaintext_key"].as_str().unwrap().to_owned();
    let key_id = issue["key_id"].as_str().unwrap().to_owned();
    assert_eq!(plaintext.len(), 43);
    assert!(plaintext
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'));
    assert_eq!(key_id.len(), 12);
    assert_eq!(count_occurrences(&issue_body, plaintext.as_bytes()), 1);

    let (status, _, keys, keys_body) =
        authed_json(app.clone(), "GET", "/admin/principals/alice/keys", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(keys["keys"].as_array().unwrap().len(), 1);
    assert_eq!(keys["keys"][0]["key_id"], key_id);
    assert_eq!(keys["keys"][0]["label"], "test-key");
    assert!(keys["keys"][0].get("plaintext_key").is_none());
    assert!(!contains_bytes(&keys_body, plaintext.as_bytes()));
    assert_forbidden_terms_absent(&keys_body);

    let (status, _, revoked, revoked_body) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/principals/alice/keys/{key_id}/revoke"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let revoked_at = revoked["revoked_at_unix_secs"].as_u64().unwrap();
    assert!(revoked_at > 0);
    assert!(!contains_bytes(&revoked_body, plaintext.as_bytes()));

    let (status, _, second, second_body) = authed_json(
        app.clone(),
        "POST",
        &format!("/admin/principals/alice/keys/{key_id}/revoke"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["revoked_at_unix_secs"], revoked_at);
    assert!(!contains_bytes(&second_body, plaintext.as_bytes()));

    let audit = storage
        .query_audit(Some("alice"), 0, u64::MAX, 100)
        .unwrap();
    let kinds = audit
        .iter()
        .filter_map(|entry| entry.kind.as_deref())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&"api_key_issue"));
    assert!(kinds.contains(&"api_key_revoke"));
    let audit_bytes = serde_json::to_vec(&audit).unwrap();
    assert!(!contains_bytes(&audit_bytes, plaintext.as_bytes()));
}

#[tokio::test]
async fn key_issue_requires_admin_auth() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(config_with_principal("alice"), Some(storage)));

    let status = unauthenticated_status(app, "POST", "/admin/principals/alice/keys").await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn non_issue_key_responses_never_return_plaintext_or_payload_terms() {
    let (_dir, storage) = temp_storage();
    let app = app(test_state(config_with_principal("alice"), Some(storage)));
    let (_, _, issue, _) = authed_json(
        app.clone(),
        "POST",
        "/admin/principals/alice/keys",
        Some(json!({ "label": "privacy" })),
    )
    .await;
    let plaintext = issue["plaintext_key"].as_str().unwrap().to_owned();
    let key_id = issue["key_id"].as_str().unwrap().to_owned();

    let (_, _, list_body) =
        authed_bytes(app.clone(), "GET", "/admin/principals/alice/keys", None).await;
    let (_, _, revoke_body) = authed_bytes(
        app,
        "POST",
        &format!("/admin/principals/alice/keys/{key_id}/revoke"),
        None,
    )
    .await;

    for body in [&list_body, &revoke_body] {
        assert_private(body);
        assert_forbidden_terms_absent(body);
        assert!(!contains_bytes(body, plaintext.as_bytes()));
    }
}

fn config_with_principal(principal_id: &str) -> Config {
    let mut config = Config::default();
    config.principals.insert(
        principal_id.to_owned(),
        PrincipalSpec {
            allowed_models: Vec::new(),
            ..PrincipalSpec::default()
        },
    );
    config
}

fn assert_forbidden_terms_absent(body: &[u8]) {
    let json: Value = serde_json::from_slice(body).unwrap();
    assert!(json.get("messages").is_none());
    let text = String::from_utf8_lossy(body);
    for forbidden in [
        "messages", "system", "tools", "tool_use", "content", "sk-ant",
    ] {
        assert!(
            !text.contains(forbidden),
            "response leaked {forbidden}: {text}"
        );
    }
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
