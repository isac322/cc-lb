use crate::common::{TestAuthn, TestState, managed_api_key};
use cc_lb_engine::authn_rail::authenticate_first;
use cc_lb_engine::codex_account::{
    AccountProjectionError, AccountProjectionMetadata, WorkspaceSwitchRequestError,
    project_codex_accounts, workspace_discovery_response, workspace_switch_target,
};
use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderValue};
use http_body_util::BodyExt;
use serde_json::{Value, json};

async fn authenticated() -> cc_lb_engine::Authenticated {
    let fixture = TestAuthn::new(TestState::default());
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", managed_api_key())).unwrap(),
    );
    authenticate_first(&fixture.authn, &headers, &fixture.principal_view)
        .await
        .expect("managed fixture authenticates")
}

fn metadata<'a>(provider_workspace_id: &'a str) -> AccountProjectionMetadata<'a> {
    AccountProjectionMetadata {
        provider_workspace_id,
        local_workspace_alias: "local-workspace",
        proxy_origin: "https://proxy.example.test/",
    }
}

fn list_account(id: &str) -> Value {
    json!({
        "id": id,
        "can_access_with_session": true,
        "sso_connection_name": null,
        "is_deactivated": false,
        "is_fedramp_compliant_workspace": false,
        "plan_type": "team",
        "workspace_backend_origin": "https://provider.example",
        "account_routing_override": "us_cr",
        "name": "Provider workspace",
        "profile_picture_url": null,
        "structure": "workspace",
        "secret_extension": "must-not-forward"
    })
}

#[tokio::test]
async fn workspace_switch_parser_returns_decoded_opaque_target() {
    let auth = authenticated().await;
    assert_eq!(
        workspace_switch_target(
            &auth,
            br#"{ "workspace_id": "local \u2603 workspace", "unknown": "secret" }"#,
        )
        .unwrap(),
        "local \u{2603} workspace"
    );
    assert_eq!(
        workspace_switch_target(&auth, br#"{ "workspace_id": "  local workspace  " }"#).unwrap(),
        "  local workspace  "
    );
}

#[tokio::test]
async fn workspace_switch_parser_rejects_invalid_requests_without_details() {
    let auth = authenticated().await;
    let secret = "workspace-secret-value";
    let invalid_bodies: &[&[u8]] = &[
        br#"{}"#,
        br#"{"workspace_id":null}"#,
        br#"{"workspace_id":7}"#,
        br#"{"workspace_id":{}}"#,
        br#"{"workspace_id":[]}"#,
        br#"{"workspace_id":""}"#,
        br#"{"workspace_id":" \t\n "}"#,
        br#"[]"#,
        br#"null"#,
        br#"{"workspace_id":"ok"} trailing"#,
        br#"{"workspace_id":"first","workspace_id":"second"}"#,
        br#"{"workspace_id":"first","\u0077orkspace_id":"second"}"#,
    ];
    for body in invalid_bodies {
        let error = workspace_switch_target(&auth, body).unwrap_err();
        assert_eq!(error, WorkspaceSwitchRequestError::InvalidRequest);
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
    }
    let malformed = format!(r#"{{"workspace_id":"{secret}"}} trailing"#);
    let error = workspace_switch_target(&auth, malformed.as_bytes()).unwrap_err();
    assert_eq!(error, WorkspaceSwitchRequestError::InvalidRequest);
    assert!(!error.to_string().contains(secret));
    assert!(!format!("{error:?}").contains(secret));
}

#[tokio::test]
async fn workspace_discovery_response_has_exact_nested_wire_shape() {
    let auth = authenticated().await;
    let aliases = vec![
        "workspace \"quoted\"".to_owned(),
        "space\\path".to_owned(),
        "유니코드 workspace".to_owned(),
        "space workspace".to_owned(),
        "space workspace".to_owned(),
    ];
    let response = workspace_discovery_response(
        &auth,
        "session \"opaque\"",
        42,
        "selected workspace",
        &aliases,
    );
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    assert!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&body).unwrap();
    let object = value.as_object().unwrap();
    let mut keys: Vec<_> = object.keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "selected_workspace_id".to_owned(),
            "session_id".to_owned(),
            "workspace_generation".to_owned(),
            "workspaces".to_owned(),
        ]
    );
    assert_eq!(object["session_id"], "session \"opaque\"");
    assert_eq!(object["workspace_generation"].as_u64(), Some(42));
    assert_eq!(object["selected_workspace_id"], "selected workspace");

    let workspaces = object["workspaces"].as_array().unwrap();
    assert_eq!(workspaces.len(), aliases.len());
    for (entry, alias) in workspaces.iter().zip(&aliases) {
        let entry = entry.as_object().unwrap();
        assert_eq!(
            entry.keys().map(|key| key.as_str()).collect::<Vec<_>>(),
            vec!["workspace_id"]
        );
        assert_eq!(entry["workspace_id"].as_str(), Some(alias.as_str()));
    }

    let empty = workspace_discovery_response(&auth, "session", 0, "selected", &[]);
    let empty_body = empty.into_body().collect().await.unwrap().to_bytes();
    let empty_value: Value = serde_json::from_slice(&empty_body).unwrap();
    assert_eq!(empty_value["workspaces"], json!([]));
}

#[tokio::test]
async fn list_projection_is_singleton_and_preserves_truthful_metadata() {
    let auth = authenticated().await;
    let upstream = json!({
        "accounts": [list_account("foreign"), list_account("provider-workspace")],
        "account_ordering": ["foreign", "provider-workspace"],
        "default_account_id": "foreign",
        "secret_top_level": "must-not-forward"
    });
    let projected =
        project_codex_accounts(&auth, upstream, &metadata("provider-workspace")).unwrap();

    assert_eq!(projected["account_ordering"], json!(["local-workspace"]));
    assert_eq!(projected["default_account_id"], "local-workspace");
    assert_eq!(projected["accounts"].as_array().unwrap().len(), 1);
    let account = &projected["accounts"][0];
    assert_eq!(account["id"], "local-workspace");
    assert_eq!(account["plan_type"], "team");
    assert_eq!(account["structure"], "workspace");
    assert_eq!(account["can_access_with_session"], true);
    assert_eq!(
        account["workspace_backend_origin"],
        "https://proxy.example.test/"
    );
    assert_eq!(account["account_routing_override"], "NO_CONSTRAINT");
    assert!(account.get("secret_extension").is_none());
    assert!(projected.get("secret_top_level").is_none());
}

#[tokio::test]
async fn list_projection_accepts_serde_default_absent_ordering() {
    let auth = authenticated().await;
    let projected = project_codex_accounts(
        &auth,
        json!({ "accounts": [list_account("provider-workspace")] }),
        &metadata("provider-workspace"),
    )
    .unwrap();
    assert_eq!(projected["accounts"][0]["id"], "local-workspace");
}

#[tokio::test]
async fn map_projection_uses_nested_identity_not_map_key() {
    let auth = authenticated().await;
    let upstream = json!({
        "accounts": {
            "cursor-a": {
                "account": {
                    "account_id": "provider-workspace",
                    "is_deactivated": false,
                    "is_fedramp_compliant_workspace": false,
                    "plan_type": "plus",
                    "name": null,
                    "profile_picture_url": null,
                    "structure": "workspace",
                    "secret_nested": "must-not-forward"
                },
                "can_access_with_session": null,
                "sso_connection_name": null,
                "secret_wrapper": "must-not-forward"
            },
            "cursor-foreign": {
                "account": {
                    "account_id": "foreign",
                    "is_deactivated": false,
                    "is_fedramp_compliant_workspace": false,
                    "plan_type": "team",
                    "structure": "workspace"
                },
                "can_access_with_session": true
            }
        },
        "account_ordering": ["cursor-foreign", "cursor-a"],
        "default_account_id": "foreign"
    });
    let projected =
        project_codex_accounts(&auth, upstream, &metadata("provider-workspace")).unwrap();

    let account = &projected["accounts"][0];
    assert_eq!(account["id"], "local-workspace");
    assert_eq!(account["plan_type"], "plus");
    assert_eq!(account["can_access_with_session"], Value::Null);
    assert_eq!(account["account_routing_override"], "NO_CONSTRAINT");
    assert!(account.get("secret_nested").is_none());
    assert!(account.get("secret_wrapper").is_none());
}

#[tokio::test]
async fn map_duplicate_cursor_and_foreign_missing_ids_follow_source_consumption() {
    let auth = authenticated().await;
    let selected = json!({
        "account": {
            "account_id": "provider-workspace",
            "is_deactivated": false,
            "is_fedramp_compliant_workspace": false,
            "plan_type": "team",
            "structure": "workspace"
        }
    });
    let projected = project_codex_accounts(
        &auth,
        json!({
            "accounts": {
                "provider-workspace": selected,
                "foreign-missing": { "account": { "account_id": null } },
                "foreign-absent": { "account": {} }
            },
            "account_ordering": [
                "provider-workspace",
                "provider-workspace",
                "foreign-missing",
                "foreign-absent"
            ]
        }),
        &metadata("provider-workspace"),
    )
    .unwrap();
    assert_eq!(projected["accounts"][0]["id"], "local-workspace");
}

#[tokio::test]
async fn absent_and_null_optional_source_fields_are_not_invented() {
    let auth = authenticated().await;
    let upstream = json!({
        "accounts": [{
            "id": "provider-workspace",
            "can_access_with_session": null,
            "sso_connection_name": null,
            "plan_type": null,
            "name": null,
            "profile_picture_url": null
        }],
        "account_ordering": []
    });
    let projected = project_codex_accounts(&auth, upstream, &metadata("provider-workspace"));
    let projected = projected.expect("list identity does not depend on ordering");
    let account = &projected["accounts"][0];
    assert_eq!(account["can_access_with_session"], Value::Null);
    assert_eq!(account["plan_type"], Value::Null);
    assert_eq!(account["name"], Value::Null);
    assert!(account.get("is_deactivated").is_none());
    assert!(account.get("structure").is_none());
}

#[tokio::test]
async fn duplicate_and_missing_selected_identities_are_rejected() {
    let auth = authenticated().await;
    let duplicate = json!({
        "accounts": [list_account("provider-workspace"), list_account("provider-workspace")],
        "account_ordering": []
    });
    assert_eq!(
        project_codex_accounts(&auth, duplicate, &metadata("provider-workspace")),
        Err(AccountProjectionError::SelectedAccountDuplicate)
    );

    let missing = json!({
        "accounts": [list_account("foreign")],
        "account_ordering": ["foreign"]
    });
    assert_eq!(
        project_codex_accounts(&auth, missing, &metadata("provider-workspace")),
        Err(AccountProjectionError::SelectedAccountMissing)
    );
}

#[tokio::test]
async fn map_duplicate_nested_identity_is_rejected() {
    let auth = authenticated().await;
    let entry = json!({
        "account": {
            "account_id": "provider-workspace",
            "is_deactivated": false,
            "is_fedramp_compliant_workspace": false,
            "plan_type": "team",
            "structure": "workspace"
        }
    });
    let upstream = json!({
        "accounts": {"a": entry.clone(), "b": entry},
        "account_ordering": ["a", "b"]
    });
    assert_eq!(
        project_codex_accounts(&auth, upstream, &metadata("provider-workspace")),
        Err(AccountProjectionError::SelectedAccountDuplicate)
    );
}

#[tokio::test]
async fn missing_selected_map_identity_is_rejected_without_fallback_to_key() {
    let auth = authenticated().await;
    let upstream = json!({
        "accounts": {"provider-workspace": {"account": {"account_id": null}}},
        "account_ordering": ["provider-workspace"]
    });
    assert_eq!(
        project_codex_accounts(&auth, upstream, &metadata("provider-workspace")),
        Err(AccountProjectionError::SelectedAccountMissing)
    );

    let malformed = json!({
        "accounts": {"provider-workspace": {"account": {"account_id": 7}}},
        "account_ordering": ["provider-workspace"]
    });
    assert_eq!(
        project_codex_accounts(&auth, malformed, &metadata("provider-workspace")),
        Err(AccountProjectionError::InvalidPayload)
    );
}

#[tokio::test]
async fn invalid_origins_are_rejected_without_echoing_input() {
    let auth = authenticated().await;
    let upstream = json!({
        "accounts": [list_account("provider-workspace")],
        "account_ordering": ["provider-workspace"]
    });
    for origin in [
        "http://proxy.example.test",
        "https://user:pass@proxy.example.test",
        "https://@proxy.example.test",
        "https://proxy.example.test/path",
        "https://proxy.example.test/./",
        "https://proxy.example.test/%2e",
        "https://proxy.example.test/?query",
        "https://proxy.example.test/#fragment",
        "https://proxy.example.test\\evil",
    ] {
        let metadata = AccountProjectionMetadata {
            provider_workspace_id: "provider-workspace",
            local_workspace_alias: "local-workspace",
            proxy_origin: origin,
        };
        let error = project_codex_accounts(&auth, upstream.clone(), &metadata).unwrap_err();
        assert_eq!(error, AccountProjectionError::InvalidOrigin);
        assert!(!error.to_string().contains(origin));
    }
}

#[tokio::test]
async fn selected_metadata_types_are_validated_and_truthful() {
    let auth = authenticated().await;
    let mut truthful = list_account("provider-workspace");
    truthful["is_deactivated"] = json!(true);
    truthful["is_fedramp_compliant_workspace"] = json!(true);
    truthful["can_access_with_session"] = json!(false);
    truthful["plan_type"] = json!("future-plan");
    truthful["structure"] = json!("future-structure");
    let projected = project_codex_accounts(
        &auth,
        json!({
            "accounts": [truthful],
            "account_ordering": ["provider-workspace"]
        }),
        &metadata("provider-workspace"),
    )
    .unwrap();
    let account = &projected["accounts"][0];
    assert_eq!(account["is_deactivated"], true);
    assert_eq!(account["is_fedramp_compliant_workspace"], true);
    assert_eq!(account["can_access_with_session"], false);
    assert_eq!(account["plan_type"], "future-plan");
    assert_eq!(account["structure"], "future-structure");

    for (field, value) in [
        ("is_deactivated", json!(null)),
        ("is_fedramp_compliant_workspace", json!("invalid")),
        ("structure", json!(null)),
        ("structure", json!(false)),
        ("can_access_with_session", json!("invalid")),
        ("plan_type", json!({"invalid": true})),
    ] {
        let mut invalid = list_account("provider-workspace");
        invalid[field] = value;
        assert_eq!(
            project_codex_accounts(
                &auth,
                json!({
                    "accounts": [invalid],
                    "account_ordering": ["provider-workspace"]
                }),
                &metadata("provider-workspace"),
            ),
            Err(AccountProjectionError::InvalidPayload)
        );
    }

    let map = json!({
        "accounts": {
            "cursor": {
                "account": {
                    "account_id": "provider-workspace",
                    "is_deactivated": true,
                    "is_fedramp_compliant_workspace": true,
                    "plan_type": "future-plan",
                    "structure": "future-structure"
                },
                "can_access_with_session": false
            }
        },
        "account_ordering": ["cursor"]
    });
    let projected = project_codex_accounts(&auth, map, &metadata("provider-workspace")).unwrap();
    assert_eq!(projected["accounts"][0]["can_access_with_session"], false);

    let invalid_map = json!({
        "accounts": {
            "cursor": {
                "account": {
                    "account_id": "provider-workspace",
                    "is_deactivated": false,
                    "is_fedramp_compliant_workspace": false,
                    "plan_type": "team",
                    "structure": "workspace"
                },
                "can_access_with_session": "invalid"
            }
        },
        "account_ordering": ["cursor"]
    });
    assert_eq!(
        project_codex_accounts(&auth, invalid_map, &metadata("provider-workspace")),
        Err(AccountProjectionError::InvalidPayload)
    );

    let missing_account = json!({
        "accounts": { "cursor": {} },
        "account_ordering": ["cursor"]
    });
    assert_eq!(
        project_codex_accounts(&auth, missing_account, &metadata("provider-workspace")),
        Err(AccountProjectionError::InvalidPayload)
    );
}

#[tokio::test]
async fn map_ordering_defaults_and_explicit_null_are_distinct() {
    let auth = authenticated().await;
    let accounts = json!({
        "provider-workspace": {
            "account": { "account_id": "provider-workspace" }
        }
    });
    assert_eq!(
        project_codex_accounts(
            &auth,
            json!({ "accounts": accounts }),
            &metadata("provider-workspace")
        ),
        Err(AccountProjectionError::SelectedAccountMissing)
    );
    assert_eq!(
        project_codex_accounts(
            &auth,
            json!({ "accounts": accounts, "account_ordering": null }),
            &metadata("provider-workspace")
        ),
        Err(AccountProjectionError::InvalidPayload)
    );
}

#[tokio::test]
async fn valid_non_default_https_port_is_retained() {
    let auth = authenticated().await;
    let upstream = json!({
        "accounts": [list_account("provider-workspace")],
        "account_ordering": ["provider-workspace"]
    });
    let metadata = AccountProjectionMetadata {
        provider_workspace_id: "provider-workspace",
        local_workspace_alias: "local-workspace",
        proxy_origin: "https://proxy.example.test:8443",
    };
    let projected = project_codex_accounts(&auth, upstream.clone(), &metadata).unwrap();
    assert_eq!(
        projected["accounts"][0]["workspace_backend_origin"],
        metadata.proxy_origin
    );

    for origin in [
        "https://192.0.2.10",
        "https://[2001:db8::10]:8443",
        "HTTPS://proxy.example.test",
    ] {
        let metadata = AccountProjectionMetadata {
            proxy_origin: origin,
            ..metadata
        };
        assert!(project_codex_accounts(&auth, upstream.clone(), &metadata).is_ok());
    }

    for metadata in [
        AccountProjectionMetadata {
            provider_workspace_id: "",
            ..metadata
        },
        AccountProjectionMetadata {
            local_workspace_alias: " ",
            ..metadata
        },
    ] {
        assert_eq!(
            project_codex_accounts(&auth, upstream.clone(), &metadata),
            Err(AccountProjectionError::InvalidMetadata)
        );
    }
}

#[tokio::test]
async fn opaque_internal_spaces_in_resolved_ids_are_preserved() {
    let auth = authenticated().await;
    let metadata = AccountProjectionMetadata {
        provider_workspace_id: "provider workspace",
        local_workspace_alias: "local workspace",
        proxy_origin: "https://proxy.example.test",
    };
    let projected = project_codex_accounts(
        &auth,
        json!({ "accounts": [{ "id": "provider workspace" }] }),
        &metadata,
    )
    .unwrap();
    assert_eq!(projected["accounts"][0]["id"], "local workspace");
}

#[tokio::test]
async fn malformed_payload_errors_are_typed_and_secret_free() {
    let auth = authenticated().await;
    let secret = "provider-secret-value";
    let upstream = json!({"accounts": secret, "secret": secret});
    let error =
        project_codex_accounts(&auth, upstream, &metadata("provider-workspace")).unwrap_err();
    assert_eq!(error, AccountProjectionError::InvalidPayload);
    assert!(!error.to_string().contains(secret));
}
