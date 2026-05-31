mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};

#[tokio::test]
async fn summary_returns_200_with_empty_storage() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/dashboard/summary?range=1h", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["range"], "1h");
    assert_eq!(body["totals"]["request_count"], 0);
    assert_eq!(body["observed"], false);
}

#[tokio::test]
async fn summary_accepts_multiple_ranges() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));
    let app = app(state);

    for range in ["15m", "1h", "6h", "24h", "7d"] {
        let (status, _, _, _) = authed_json(
            app.clone(),
            "GET",
            &format!("/admin/dashboard/summary?range={range}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "range {range} should succeed");
    }
}

#[tokio::test]
async fn summary_rejects_invalid_range() {
    let (_dir, storage) = temp_storage();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/dashboard/summary?range=42q",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn summary_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) =
        authed_bytes(app(state), "GET", "/admin/dashboard/summary?range=1h", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
