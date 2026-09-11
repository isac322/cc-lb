use crate::config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::{RequestEvent, RequestEventStore, UsageRollupStore};
use config_admin_common::{
    app, authed_bytes, authed_json, temp_storage, temp_storage_with_clock, test_state,
    test_state_with_clock,
};
use uuid::Uuid;

const UNALIGNED_NOW_UNIX_SECS: u64 = 1_700_000_000;
const ALIGNED_HOUR_UNIX_SECS: u64 = 1_700_002_800;
const ALIGNED_MINUTE_UNIX_SECS: u64 = ALIGNED_HOUR_UNIX_SECS + 60;

#[tokio::test]
async fn t2__summary_returns_200_with_empty_storage() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/dashboard/summary?range=1h", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["range"], "1h");
    assert_eq!(body["totals"]["request_count"], 0);
    assert_eq!(body["observed"], false);
}

#[tokio::test]
async fn t2__summary_accepts_multiple_ranges() {
    let (_dir, storage) = temp_storage().await;
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
async fn t2__summary_rejects_invalid_range() {
    let (_dir, storage) = temp_storage().await;
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
async fn t2__summary_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) =
        authed_bytes(app(state), "GET", "/admin/dashboard/summary?range=1h", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn t3__summary_includes_current_partial_minute_and_hour_rollups() {
    let clock = test_clock(UNALIGNED_NOW_UNIX_SECS);
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    storage
        .append_request_event(&usage_event(UNALIGNED_NOW_UNIX_SECS))
        .await
        .unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let admin_app = app(state);
    let cases = [
        ("1h", 60_u64 * 60, 60_usize),
        ("6h", 6 * 60 * 60, 360),
        ("24h", 24 * 60 * 60, 24),
        ("7d", 7 * 24 * 60 * 60, 168),
    ];

    for (range, range_secs, expected_bucket_count) in cases {
        let (status, _, body, _) = authed_json(
            admin_app.clone(),
            "GET",
            &format!("/admin/dashboard/summary?range={range}"),
            None,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let width = range_secs / expected_bucket_count as u64;
        let current_bucket_start = UNALIGNED_NOW_UNIX_SECS - (UNALIGNED_NOW_UNIX_SECS % width);
        let expected_end = current_bucket_start + width;
        assert_eq!(body["window_end_unix_secs"], expected_end);
        assert_eq!(body["window_start_unix_secs"], expected_end - range_secs);
        assert_eq!(body["totals"]["request_count"], 1);

        let buckets = body["sparkline"]["buckets"]
            .as_array()
            .expect("sparkline buckets");
        assert_eq!(buckets.len(), expected_bucket_count);
        assert_eq!(
            buckets.last().expect("current bucket")["bucket_start_unix_secs"],
            current_bucket_start
        );
        assert_eq!(buckets.last().expect("current bucket")["request_count"], 1);
    }
}

#[tokio::test]
async fn t2__summary_exact_boundaries_do_not_advance_or_change_bucket_count() {
    let minute_clock = test_clock(ALIGNED_MINUTE_UNIX_SECS);
    let (_minute_dir, minute_storage) = temp_storage_with_clock(minute_clock.clone()).await;
    let minute_app = app(test_state_with_clock(
        Config::default(),
        Some(minute_storage),
        minute_clock,
    ));
    let hour_clock = test_clock(ALIGNED_HOUR_UNIX_SECS);
    let (_hour_dir, hour_storage) = temp_storage_with_clock(hour_clock.clone()).await;
    let hour_app = app(test_state_with_clock(
        Config::default(),
        Some(hour_storage),
        hour_clock,
    ));
    let cases = [
        (
            minute_app.clone(),
            ALIGNED_MINUTE_UNIX_SECS,
            "1h",
            60_u64 * 60,
            60_usize,
        ),
        (minute_app, ALIGNED_MINUTE_UNIX_SECS, "6h", 6 * 60 * 60, 360),
        (
            hour_app.clone(),
            ALIGNED_HOUR_UNIX_SECS,
            "24h",
            24 * 60 * 60,
            24,
        ),
        (
            hour_app,
            ALIGNED_HOUR_UNIX_SECS,
            "7d",
            7 * 24 * 60 * 60,
            168,
        ),
    ];

    for (admin_app, now_unix_secs, range, range_secs, expected_bucket_count) in cases {
        let (status, _, body, _) = authed_json(
            admin_app,
            "GET",
            &format!("/admin/dashboard/summary?range={range}"),
            None,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["window_end_unix_secs"], now_unix_secs);
        assert_eq!(body["window_start_unix_secs"], now_unix_secs - range_secs);

        let buckets = body["sparkline"]["buckets"]
            .as_array()
            .expect("sparkline buckets");
        assert_eq!(buckets.len(), expected_bucket_count);
        let width = range_secs / expected_bucket_count as u64;
        assert_eq!(
            buckets.last().expect("last completed bucket")["bucket_start_unix_secs"],
            now_unix_secs - width
        );
    }
}

fn test_clock(now_unix_secs: u64) -> ClockHandle {
    Arc::new(TestClock::new_at_secs(now_unix_secs))
}

fn usage_event(ts: u64) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(ts * 1_000),
        request_id: "current-partial-bucket".to_owned(),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some("test-key".to_owned()),
        upstream_id: Some(Uuid::from_u128(1)),
        upstream_name: Some("test-upstream".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(0),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(0),
        cost_usd_micros: Some(0),
        duration_ms: 10,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..Default::default()
    }
}
