use crate::config_admin_common;

use axum::http::StatusCode;
use config_admin_common::{
    app, authed_json, config_value, expected_revision_body, put_body, temp_storage, test_state,
    test_state_without_storage,
};
use serde_json::json;

#[tokio::test]
async fn config_validate_table() {
    enum Expected {
        InvalidDraft,
        ValidDraft,
        InvalidUpstreamAffinityTtl,
    }

    struct Case {
        case: &'static str,
        draft: fn() -> serde_json::Value,
        expected: Expected,
    }

    let cases = [
        Case {
            case: "invalid_draft_validate_reports_false_and_keeps_last_validated_revision",
            draft: || json!({ "upstreams": "wrong-type" }),
            expected: Expected::InvalidDraft,
        },
        Case {
            case: "valid_draft_validate_marks_current_revision_valid",
            draft: || config_value(123),
            expected: Expected::ValidDraft,
        },
        Case {
            case: "zero_upstream_affinity_ttl_reports_exact_validation_path",
            draft: || {
                let mut draft = config_value(123);
                draft["upstream_affinity"]["ttl_days"] = json!(0);
                draft
            },
            expected: Expected::InvalidUpstreamAffinityTtl,
        },
    ];

    for case in cases {
        let (_dir, storage) = temp_storage().await;
        let app = app(test_state(
            config_admin_common::minimal_config(),
            Some(storage),
        ));

        let _ = authed_json(
            app.clone(),
            "PUT",
            "/admin/config/draft",
            Some(put_body((case.draft)(), 0)),
        )
        .await;
        let (status, _, response, _) = authed_json(
            app.clone(),
            "POST",
            "/admin/config/draft/validate",
            Some(expected_revision_body(1)),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "case={}", case.case);
        match case.expected {
            Expected::InvalidDraft => {
                assert_eq!(response["valid"], false, "case={}", case.case);
                assert_eq!(response["revision"], 1, "case={}", case.case);
                assert!(
                    !response["error"].as_str().unwrap().is_empty(),
                    "case={}",
                    case.case
                );

                let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
                assert_eq!(
                    draft["last_validated_revision"],
                    serde_json::Value::Null,
                    "case={}",
                    case.case
                );
                assert!(
                    !draft["last_validation_error"].as_str().unwrap().is_empty(),
                    "case={}",
                    case.case
                );
            }
            Expected::ValidDraft => {
                assert_eq!(response["valid"], true, "case={}", case.case);
                assert_eq!(response["revision"], 1, "case={}", case.case);
                assert!(response.get("error").is_none(), "case={}", case.case);

                let (_, _, draft, _) = authed_json(app, "GET", "/admin/config/draft", None).await;
                assert_eq!(draft["last_validated_revision"], 1, "case={}", case.case);
                assert_eq!(
                    draft["last_validation_error"],
                    serde_json::Value::Null,
                    "case={}",
                    case.case
                );
            }
            Expected::InvalidUpstreamAffinityTtl => {
                assert_eq!(response["valid"], false, "case={}", case.case);
                assert!(
                    response["error"]
                        .as_str()
                        .unwrap()
                        .contains("upstream_affinity.ttl_days"),
                    "case={}",
                    case.case
                );
            }
        }
    }
}

#[tokio::test]
async fn t3__stale_validate_revision_returns_conflict() {
    let (_dir, storage) = crate::config_admin_common::sqlite_temp_storage().await;
    let app = app(test_state(
        config_admin_common::minimal_config(),
        Some(storage),
    ));

    let _ = authed_json(
        app.clone(),
        "PUT",
        "/admin/config/draft",
        Some(put_body(config_value(123), 0)),
    )
    .await;
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(0)),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "stale_draft_revision");
}

#[tokio::test]
async fn t2__validate_without_storage_returns_unavailable() {
    let app = app(test_state_without_storage());
    let (status, _, json, _) = authed_json(
        app,
        "POST",
        "/admin/config/draft/validate",
        Some(expected_revision_body(0)),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["valid"], false);
    assert_eq!(json["error"], "draft_missing");
}
