mod admin_test_common;

use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn subscription_quota_latest_is_registered_on_v1_and_legacy_paths() {
    let server = admin_test_common::spawn_admin_server();
    let (status, _, body) = server
        .client
        .post_json(
            "/admin/v1/upstreams",
            json!({ "name": "quota-upstream", "kind": "anthropic_oauth" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let upstream_id = body["id"].as_str().unwrap();

    for path in [
        "/admin/v1/subscription-quotas/latest",
        "/admin/subscription-quotas/latest",
    ] {
        let (status, _, body) = server.client.get(path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(body["upstreams"][0]["upstream_id"], upstream_id);
        assert_eq!(body["upstreams"][0]["upstream_name"], "quota-upstream");
    }
}

#[tokio::test]
async fn subscription_quota_series_defaults_missing_upstream_ids_to_all() {
    let server = admin_test_common::spawn_admin_server();
    let (status, _, _) = server
        .client
        .post_json(
            "/admin/v1/upstreams",
            json!({ "name": "quota-series", "kind": "anthropic_oauth" }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, body) = server
        .client
        .get("/admin/v1/subscription-quotas/series?since_unix_secs=1&until_unix_secs=120")
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "merged");
    assert_eq!(body["bucket_secs"], 300);
    assert!(body["series"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn subscription_quota_analysis_defaults_missing_upstream_ids_to_all() {
    let server = admin_test_common::spawn_admin_server();

    let (status, _, _) = server
        .client
        .get("/admin/v1/subscription-quotas/analysis?since_unix_secs=1&until_unix_secs=120")
        .await;

    assert_eq!(status, StatusCode::OK);
}
