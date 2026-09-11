use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use serde_json::{Value, json};

use crate::config_admin_common::{app, authed_bytes, authed_json, temp_storage};

use super::{NOW_UNIX_SECS, fixtures::principal_create_body};

#[tokio::test]
async fn t2__cache_keepalive_disabled_principal_and_empty_status_return_empty_rows_without_mutation()
 {
    // Given: a principal whose Cache keepalive configuration is disabled and has no history.
    let (_directory, storage) = temp_storage().await;
    let clock: ClockHandle = Arc::new(TestClock::new_at_secs(NOW_UNIX_SECS));
    let state =
        crate::config_admin_common::test_state_with_clock(Config::default(), Some(storage), clock);
    let mut body = principal_create_body();
    body["cache_keepalive"]["enabled"] = json!(false);
    let (status, _headers, created, _raw) = authed_json(
        app(state.clone()),
        "POST",
        "/admin/v1/principals",
        Some(body),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "disabled principal create: {created:?}"
    );
    let principal_id = created["id"].as_str().expect("disabled principal id");

    // When: the card summary and an unmatched state filter are fetched.
    let summary_uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?limit=0");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &summary_uri, None).await;
    assert_eq!(status, StatusCode::OK, "disabled summary: {raw:?}");
    let summary: Value = serde_json::from_slice(&raw).expect("disabled summary JSON");
    let empty_uri = format!("/admin/v1/principals/{principal_id}/cache-keepalive?status=expired");
    let (status, _headers, raw) = authed_bytes(app(state.clone()), "GET", &empty_uri, None).await;

    // Then: read endpoints remain non-mutating and distinguish empty result data.
    assert_eq!(status, StatusCode::OK, "empty status filter: {raw:?}");
    let empty: Value = serde_json::from_slice(&raw).expect("empty response JSON");
    assert_eq!(summary["rows"], json!([]));
    assert_eq!(empty["rows"], json!([]));
    assert_eq!(summary["summary"]["renewing_now"], 0);
    let (status, _headers, principal, _raw) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/principals/{principal_id}"),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "disabled principal read: {principal:?}"
    );
    assert_eq!(principal["cache_keepalive"]["enabled"], false);
}
