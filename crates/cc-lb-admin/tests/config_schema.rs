use crate::config_admin_common;

use axum::http::StatusCode;
use config_admin_common::{app, authed_json, test_state_without_storage, unauthenticated_status};

#[tokio::test]
async fn config_schema_table() {
    enum Check {
        SchemaAndChecklist,
        UpstreamAffinity,
        CoverageProperties,
        CacheControl,
        RequiresAuth,
    }

    struct Case {
        case: &'static str,
        authenticated: bool,
        check: Check,
    }

    let cases = [
        Case {
            case: "authorized_schema_returns_schema_and_checklist",
            authenticated: true,
            check: Check::SchemaAndChecklist,
        },
        Case {
            case: "schema_and_coverage_include_upstream_affinity",
            authenticated: true,
            check: Check::UpstreamAffinity,
        },
        Case {
            case: "schema_contains_property_for_each_coverage_item",
            authenticated: true,
            check: Check::CoverageProperties,
        },
        Case {
            case: "schema_response_is_cacheable_for_sixty_seconds",
            authenticated: true,
            check: Check::CacheControl,
        },
        Case {
            case: "schema_requires_admin_auth",
            authenticated: false,
            check: Check::RequiresAuth,
        },
    ];

    for case in cases {
        let app = app(test_state_without_storage());
        if case.authenticated {
            let (status, headers, json, _) =
                authed_json(app, "GET", "/admin/config/schema", None).await;

            match case.check {
                Check::SchemaAndChecklist => {
                    assert_eq!(status, StatusCode::OK, "case={}", case.case);
                    assert!(json.get("schema").is_some(), "case={}", case.case);
                    assert!(
                        json["coverage_checklist"].as_array().unwrap().len() >= 16,
                        "case={}",
                        case.case
                    );
                }
                Check::UpstreamAffinity => {
                    assert!(
                        json["schema"]["properties"]
                            .as_object()
                            .unwrap()
                            .contains_key("upstream_affinity"),
                        "case={}",
                        case.case
                    );
                    assert!(
                        json["coverage_checklist"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|item| item == "upstream_affinity"),
                        "case={}",
                        case.case
                    );
                    assert_eq!(
                        json["schema"]["$defs"]["UpstreamAffinityConfig"]["properties"]["ttl_days"]
                            ["minimum"],
                        1,
                        "case={}",
                        case.case
                    );
                }
                Check::CoverageProperties => {
                    let properties = json["schema"]["properties"].as_object().unwrap();
                    for item in json["coverage_checklist"].as_array().unwrap() {
                        let name = item.as_str().unwrap();
                        assert!(
                            properties.contains_key(name),
                            "missing schema property {name}; case={}",
                            case.case
                        );
                    }
                }
                Check::CacheControl => {
                    assert_eq!(status, StatusCode::OK, "case={}", case.case);
                    assert_eq!(
                        headers.get("cache-control").unwrap(),
                        "max-age=60",
                        "case={}",
                        case.case
                    );
                }
                Check::RequiresAuth => unreachable!("case={}", case.case),
            }
        } else {
            let status = unauthenticated_status(app, "GET", "/admin/config/schema").await;
            assert!(
                matches!(case.check, Check::RequiresAuth),
                "case={}",
                case.case
            );
            assert_eq!(status, StatusCode::UNAUTHORIZED, "case={}", case.case);
        }
    }
}
