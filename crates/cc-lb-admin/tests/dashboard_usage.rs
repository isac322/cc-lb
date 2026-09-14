use crate::config_admin_common;

use std::sync::Arc;

use axum::http::{StatusCode, header};
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_storage_api::{
    RequestEvent, RequestEventStore, UsageRollupStore, normalize_usage_rollup_dimension,
};
use config_admin_common::{
    app, authed_bytes, authed_bytes_with_headers, authed_json, temp_storage, test_state,
    test_state_with_clock,
};
use serde_json::Value;
use uuid::Uuid;

const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;

#[tokio::test]
async fn t2__usage_returns_200_grouped_by_model() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["range"], "1h");
    assert_eq!(body["group_by"], "model");
}

#[tokio::test]
async fn t3__dashboard_etags_short_circuit_rollup_scans_before_response_building() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let now = current_unix_secs(clock.as_ref());
    storage
        .append_request_event(&usage_event(
            now.saturating_sub(60),
            "req-dashboard-etag",
            Uuid::from_u128(1),
            "etag-upstream",
            1,
        ))
        .await
        .unwrap();
    storage.rollup_usage_once().await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage.clone()),
        clock,
    ));
    let usage_uri = "/admin/usage?range=1h&step=minute&group_by=model";
    let principal_uri = "/admin/usage?range=1h&step=minute&group_by=principal&projection=totals";
    let summary_uri = "/admin/dashboard/summary?range=1h";
    let (usage_status, usage_headers, _) =
        authed_bytes(admin_app.clone(), "GET", usage_uri, None).await;
    let (summary_status, summary_headers, _) =
        authed_bytes(admin_app.clone(), "GET", summary_uri, None).await;
    let (principal_status, principal_headers, _) =
        authed_bytes(admin_app.clone(), "GET", principal_uri, None).await;
    assert_eq!(usage_status, StatusCode::OK);
    assert_eq!(summary_status, StatusCode::OK);
    assert_eq!(principal_status, StatusCode::OK);
    assert!(
        !principal_headers.contains_key(header::ETAG),
        "principal cost responses depend on unrolled request events"
    );
    let usage_etag = usage_headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("usage ETag")
        .to_owned();
    let summary_etag = summary_headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("summary ETag")
        .to_owned();
    assert!(usage_etag.starts_with("W/\"dashboard:usage:"));
    assert!(summary_etag.starts_with("W/\"dashboard:summary:"));
    assert_eq!(
        usage_headers
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-cache")
    );

    storage
        .append_request_event(&usage_event(
            now.saturating_sub(30),
            "req-dashboard-etag-next",
            Uuid::from_u128(1),
            "etag-upstream",
            2,
        ))
        .await
        .unwrap();
    storage.rollup_usage_once().await.unwrap();
    let (usage_status, refreshed_usage_headers, _) = authed_bytes_with_headers(
        admin_app.clone(),
        "GET",
        usage_uri,
        None,
        &[(header::IF_NONE_MATCH.as_str(), usage_etag.as_str())],
    )
    .await;
    let (summary_status, refreshed_summary_headers, _) = authed_bytes_with_headers(
        admin_app.clone(),
        "GET",
        summary_uri,
        None,
        &[(header::IF_NONE_MATCH.as_str(), summary_etag.as_str())],
    )
    .await;
    assert_eq!(usage_status, StatusCode::OK);
    assert_eq!(summary_status, StatusCode::OK);
    let refreshed_usage_etag = refreshed_usage_headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("refreshed usage ETag")
        .to_owned();
    let refreshed_summary_etag = refreshed_summary_headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .expect("refreshed summary ETag")
        .to_owned();
    assert_ne!(refreshed_usage_etag, usage_etag);
    assert_ne!(refreshed_summary_etag, summary_etag);

    sqlx::query("DROP TABLE usage_rollups_v2")
        .execute(storage.pool())
        .await
        .unwrap();
    let (usage_status, usage_headers, usage_body) = authed_bytes_with_headers(
        admin_app.clone(),
        "GET",
        usage_uri,
        None,
        &[(
            header::IF_NONE_MATCH.as_str(),
            refreshed_usage_etag.as_str(),
        )],
    )
    .await;
    assert_eq!(usage_status, StatusCode::NOT_MODIFIED);
    assert!(usage_body.is_empty());
    assert_eq!(
        usage_headers
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(refreshed_usage_etag.as_str())
    );

    let (summary_status, _, summary_body) = authed_bytes_with_headers(
        admin_app,
        "GET",
        summary_uri,
        None,
        &[(
            header::IF_NONE_MATCH.as_str(),
            refreshed_summary_etag.as_str(),
        )],
    )
    .await;
    assert_eq!(summary_status, StatusCode::NOT_MODIFIED);
    assert!(summary_body.is_empty());
}

#[tokio::test]
async fn t2__usage_legacy_dashboard_alias_matches_v1_body() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let admin_app = app(state);

    let (legacy_status, _, legacy_body, _) = authed_json(
        admin_app.clone(),
        "GET",
        "/admin/dashboard/usage?range=1h&group_by=model",
        None,
    )
    .await;
    let (v1_status, _, v1_body, _) = authed_json(
        admin_app,
        "GET",
        "/admin/v1/dashboard/usage?range=1h&group_by=model",
        None,
    )
    .await;

    assert_eq!(legacy_status, StatusCode::OK);
    assert_eq!(v1_status, StatusCode::OK);
    assert_eq!(legacy_body, v1_body);
}

#[tokio::test]
async fn t2__usage_returns_200_grouped_by_principal() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["group_by"], "principal");
}

#[tokio::test]
async fn t3__usage_totals_projection_preserves_full_series_totals() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(10);
    let now = current_unix_secs(clock.as_ref());

    let mut older = usage_event(
        now.saturating_sub(120),
        "req-totals-older",
        upstream_id,
        "target-upstream",
        3,
    );
    older.cost_usd_micros = Some(15);
    older.cost_input_micros = Some(1);
    older.cost_output_micros = Some(2);
    older.cost_cache_creation_5m_micros = Some(3);
    older.cost_cache_creation_1h_micros = Some(4);
    older.cost_cache_read_micros = Some(5);
    storage.append_request_event(&older).await.unwrap();

    let mut newer = usage_event(
        now.saturating_sub(60),
        "req-totals-newer",
        upstream_id,
        "target-upstream",
        7,
    );
    newer.status = 500;
    newer.cost_usd_micros = Some(10);
    newer.cost_input_micros = Some(2);
    newer.cost_output_micros = Some(3);
    newer.cost_cache_creation_5m_micros = Some(0);
    newer.cost_cache_creation_1h_micros = Some(0);
    newer.cost_cache_read_micros = Some(5);
    newer.duration_ms = 25;
    storage.append_request_event(&newer).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));
    let (full_status, _, full, _) = authed_json(
        admin_app.clone(),
        "GET",
        "/admin/usage?range=1h&step=minute&group_by=principal",
        None,
    )
    .await;
    let (totals_status, _, totals, _) = authed_json(
        admin_app,
        "GET",
        "/admin/usage?range=1h&step=minute&group_by=principal&projection=totals",
        None,
    )
    .await;

    assert_eq!(full_status, StatusCode::OK);
    assert_eq!(totals_status, StatusCode::OK);
    assert_usage_totals_match_full_collapse(&full, &totals);
    assert!(
        full["series"][0]["buckets"].as_array().unwrap().len() > 1,
        "the default response must retain its dense full-series contract"
    );
}

#[tokio::test]
async fn t3__usage_totals_projection_preserves_components_across_rollup_lag() {
    let clock = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(11);
    let now = current_unix_secs(clock.as_ref());

    let mut rolled = usage_event(
        now.saturating_sub(120),
        "req-totals-lag-rolled",
        upstream_id,
        "target-upstream",
        1,
    );
    rolled.cost_usd_micros = Some(10);
    rolled.cost_input_micros = Some(1);
    rolled.cost_output_micros = Some(2);
    rolled.cost_cache_creation_5m_micros = Some(3);
    rolled.cost_cache_creation_1h_micros = Some(4);
    rolled.cost_cache_read_micros = Some(0);
    storage.append_request_event(&rolled).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage.clone()),
        clock.clone(),
    ));
    let uri = "/admin/usage?range=1h&step=minute&group_by=principal";
    let (baseline_full, baseline_totals) = usage_projection_pair(&admin_app, uri).await;
    assert_usage_totals_match_full_collapse(&baseline_full, &baseline_totals);
    assert_usage_bucket_fields(
        &baseline_totals["series"][0]["buckets"][0],
        &[
            ("request_count", 1),
            ("input_tokens", 1),
            ("virtual_cost_micros", 10),
            ("cost_input_micros", 1),
            ("cost_output_micros", 2),
            ("cost_cache_creation_5m_micros", 3),
            ("cost_cache_creation_1h_micros", 4),
            ("cost_cache_read_micros", 0),
        ],
    );

    let mut unrolled = rolled.clone();
    unrolled.request_id = "req-totals-lag-unrolled".to_owned();
    unrolled.event_id = None;
    unrolled.ts_ms = Some(now.saturating_sub(1) * 1_000);
    unrolled.cost_usd_micros = Some(5);
    unrolled.cost_input_micros = Some(5);
    unrolled.cost_output_micros = Some(0);
    unrolled.cost_cache_creation_5m_micros = Some(0);
    unrolled.cost_cache_creation_1h_micros = Some(0);
    unrolled.cost_cache_read_micros = Some(0);
    storage.append_request_event(&unrolled).await.unwrap();
    clock.advance_secs(60);

    let (live_full, live_totals) = usage_projection_pair(&admin_app, uri).await;
    assert_usage_totals_match_full_collapse(&live_full, &live_totals);
    assert_usage_bucket_fields(
        &live_totals["series"][0]["buckets"][0],
        &[
            ("request_count", 1),
            ("input_tokens", 1),
            ("virtual_cost_micros", 10),
            ("cost_input_micros", 1),
            ("cost_output_micros", 2),
            ("cost_cache_creation_5m_micros", 3),
            ("cost_cache_creation_1h_micros", 4),
            ("cost_cache_read_micros", 0),
        ],
    );

    storage.rollup_usage_once().await.unwrap();
    clock.advance_secs(60);
    let (caught_up_full, caught_up_totals) = usage_projection_pair(&admin_app, uri).await;
    assert_usage_totals_match_full_collapse(&caught_up_full, &caught_up_totals);
    assert_usage_bucket_fields(
        &caught_up_totals["series"][0]["buckets"][0],
        &[
            ("request_count", 2),
            ("input_tokens", 2),
            ("virtual_cost_micros", 15),
            ("cost_input_micros", 6),
            ("cost_output_micros", 2),
            ("cost_cache_creation_5m_micros", 3),
            ("cost_cache_creation_1h_micros", 4),
            ("cost_cache_read_micros", 0),
        ],
    );
}

#[tokio::test]
async fn t3__usage_totals_projection_preserves_unrolled_recorded_zero_components() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(15);
    let now = current_unix_secs(clock.as_ref());

    let rolled = usage_event(
        now.saturating_sub(120),
        "req-totals-zero-rolled",
        upstream_id,
        "target-upstream",
        1,
    );
    storage.append_request_event(&rolled).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let mut zero_tail = rolled.clone();
    zero_tail.request_id = "req-totals-zero-tail".to_owned();
    zero_tail.event_id = None;
    zero_tail.ts_ms = Some(now.saturating_sub(1) * 1_000);
    zero_tail.cost_input_micros = Some(0);
    zero_tail.cost_output_micros = Some(0);
    zero_tail.cost_cache_creation_5m_micros = Some(0);
    zero_tail.cost_cache_creation_1h_micros = Some(0);
    zero_tail.cost_cache_read_micros = Some(0);
    storage.append_request_event(&zero_tail).await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));
    let (full, totals) = usage_projection_pair(
        &admin_app,
        "/admin/usage?range=1h&step=minute&group_by=principal",
    )
    .await;
    assert_usage_totals_match_full_collapse(&full, &totals);
    assert_usage_bucket_fields(
        &totals["series"][0]["buckets"][0],
        &[
            ("request_count", 1),
            ("input_tokens", 1),
            ("virtual_cost_micros", 0),
            ("cost_input_micros", 0),
            ("cost_output_micros", 0),
            ("cost_cache_creation_5m_micros", 0),
            ("cost_cache_creation_1h_micros", 0),
            ("cost_cache_read_micros", 0),
        ],
    );
}

#[tokio::test]
async fn t3__usage_totals_projection_matches_upstream_filter_and_empty_transition() {
    let clock = Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS));
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let target_upstream_id = Uuid::from_u128(12);
    let other_upstream_id = Uuid::from_u128(13);
    let initially_empty_upstream_id = Uuid::from_u128(14);
    let now = current_unix_secs(clock.as_ref());

    let mut target = usage_event(
        now.saturating_sub(120),
        "req-totals-filter-target",
        target_upstream_id,
        "target-upstream",
        3,
    );
    target.output_tokens = Some(5);
    target.cache_creation_input_tokens = Some(7);
    target.cache_read_input_tokens = Some(11);
    target.cost_usd_micros = Some(15);
    target.cost_input_micros = Some(1);
    target.cost_output_micros = Some(2);
    target.cost_cache_creation_5m_micros = Some(3);
    target.cost_cache_creation_1h_micros = Some(4);
    target.cost_cache_read_micros = Some(5);
    storage.append_request_event(&target).await.unwrap();

    let mut other = usage_event(
        now.saturating_sub(60),
        "req-totals-filter-other",
        other_upstream_id,
        "other-upstream",
        100,
    );
    other.principal_id = Some("principal-b".to_owned());
    other.cost_usd_micros = Some(100);
    other.cost_input_micros = Some(100);
    storage.append_request_event(&other).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage.clone()),
        clock.clone(),
    ));
    let target_uri = format!(
        "/admin/usage?range=1h&step=minute&group_by=principal&upstream_id={target_upstream_id}"
    );
    let (target_full, target_totals) = usage_projection_pair(&admin_app, target_uri.as_str()).await;
    assert_usage_totals_match_full_collapse(&target_full, &target_totals);
    assert_eq!(target_totals["series"].as_array().unwrap().len(), 1);
    assert_eq!(target_totals["series"][0]["key"], "principal-a");
    assert_usage_bucket_fields(
        &target_totals["series"][0]["buckets"][0],
        &[
            ("request_count", 1),
            ("input_tokens", 3),
            ("output_tokens", 5),
            ("cache_creation_input_tokens", 7),
            ("cache_read_input_tokens", 11),
            ("virtual_cost_micros", 15),
            ("cost_input_micros", 1),
            ("cost_output_micros", 2),
            ("cost_cache_creation_5m_micros", 3),
            ("cost_cache_creation_1h_micros", 4),
            ("cost_cache_read_micros", 5),
        ],
    );

    let empty_uri = format!(
        "/admin/usage?range=1h&step=minute&group_by=principal&upstream_id={initially_empty_upstream_id}"
    );
    let (empty_full, empty_totals) = usage_projection_pair(&admin_app, empty_uri.as_str()).await;
    assert_usage_totals_match_full_collapse(&empty_full, &empty_totals);
    assert_eq!(empty_totals["observed"], false);
    assert!(empty_totals["series"].as_array().unwrap().is_empty());

    let mut newly_observed = usage_event(
        now.saturating_sub(1),
        "req-totals-filter-new",
        initially_empty_upstream_id,
        "new-upstream",
        17,
    );
    newly_observed.principal_id = Some("principal-c".to_owned());
    newly_observed.cost_usd_micros = Some(9);
    newly_observed.cost_input_micros = Some(9);
    storage.append_request_event(&newly_observed).await.unwrap();
    storage.rollup_usage_once().await.unwrap();
    clock.advance_secs(60);

    let (observed_full, observed_totals) =
        usage_projection_pair(&admin_app, empty_uri.as_str()).await;
    assert_usage_totals_match_full_collapse(&observed_full, &observed_totals);
    assert_eq!(observed_totals["observed"], true);
    assert_eq!(observed_totals["series"][0]["key"], "principal-c");
    assert_usage_bucket_fields(
        &observed_totals["series"][0]["buckets"][0],
        &[
            ("request_count", 1),
            ("input_tokens", 17),
            ("virtual_cost_micros", 9),
            ("cost_input_micros", 9),
        ],
    );
}

#[tokio::test]
async fn t2__usage_rejects_invalid_projection() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal&projection=dense",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn t2__usage_rejects_invalid_group_by() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=bogus",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn t3__usage_filters_by_upstream_id() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(3_600);

    storage
        .append_request_event(&usage_event(
            bucket_ts,
            "req-target",
            upstream_id,
            "target-upstream",
            3,
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&usage_event(
            bucket_ts,
            "req-other",
            other_upstream_id,
            "other-upstream",
            7,
        ))
        .await
        .unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/usage?range=24h&group_by=model&upstream_id={upstream_id}"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let series = body["series"].as_array().expect("series array");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["key"], "claude-sonnet-4-5");
    let input_tokens = series[0]["buckets"]
        .as_array()
        .expect("buckets array")
        .iter()
        .map(|bucket| bucket["input_tokens"].as_u64().unwrap_or_default())
        .sum::<u64>();
    assert_eq!(input_tokens, 3);
}

#[tokio::test]
async fn t3__usage_principal_enriches_mixed_legacy_and_component_costs() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(3);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);

    let mut modern = usage_event(
        bucket_ts,
        "req-principal-modern",
        upstream_id,
        "target-upstream",
        1,
    );
    modern.principal_id = Some("  principal/A  ".to_owned());
    modern.source_kind = Some("renewal".to_owned());
    modern.cost_usd_micros = Some(15);
    modern.cost_input_micros = Some(1);
    modern.cost_output_micros = Some(2);
    modern.cost_cache_creation_5m_micros = Some(3);
    modern.cost_cache_creation_1h_micros = Some(4);
    modern.cost_cache_read_micros = Some(5);
    storage.append_request_event(&modern).await.unwrap();

    let mut legacy = usage_event(
        bucket_ts + 1,
        "req-principal-legacy",
        upstream_id,
        "target-upstream",
        1,
    );
    legacy.principal_id = Some("principal A".to_owned());
    legacy.cost_usd_micros = Some(10);
    storage.append_request_event(&legacy).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let principal = normalize_usage_rollup_dimension(Some("principal A"));
    let series = body["series"]
        .as_array()
        .unwrap()
        .iter()
        .find(|series| series["key"].as_str() == Some(principal.as_str()))
        .expect("normalized principal series");
    let bucket = series["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["virtual_cost_micros"] == 25)
        .expect("cost-bearing bucket");
    assert_eq!(bucket["cost_input_micros"], 1);
    assert_eq!(bucket["cost_output_micros"], 2);
    assert_eq!(bucket["cost_cache_creation_5m_micros"], 3);
    assert_eq!(bucket["cost_cache_creation_1h_micros"], 4);
    assert_eq!(bucket["cost_cache_read_micros"], 5);
}

#[tokio::test]
async fn t3__usage_principal_preserves_recorded_zero_component_costs() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(4);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);
    let mut event = usage_event(
        bucket_ts,
        "req-principal-zero",
        upstream_id,
        "target-upstream",
        1,
    );
    event.cost_input_micros = Some(0);
    event.cost_output_micros = Some(0);
    event.cost_cache_creation_5m_micros = Some(0);
    event.cost_cache_creation_1h_micros = Some(0);
    event.cost_cache_read_micros = Some(0);
    storage.append_request_event(&event).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let bucket = body["series"][0]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["request_count"] == 1)
        .expect("observed bucket");
    assert_eq!(bucket["cost_input_micros"], 0);
    assert_eq!(bucket["cost_output_micros"], 0);
    assert_eq!(bucket["cost_cache_creation_5m_micros"], 0);
    assert_eq!(bucket["cost_cache_creation_1h_micros"], 0);
    assert_eq!(bucket["cost_cache_read_micros"], 0);
}

#[tokio::test]
async fn t3__usage_principal_omits_components_when_request_events_are_ahead() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(5);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);
    let mut rolled = usage_event(
        bucket_ts,
        "req-principal-rolled",
        upstream_id,
        "target-upstream",
        1,
    );
    rolled.cost_usd_micros = Some(10);
    rolled.cost_input_micros = Some(10);
    rolled.cost_output_micros = Some(0);
    rolled.cost_cache_creation_5m_micros = Some(0);
    rolled.cost_cache_creation_1h_micros = Some(0);
    rolled.cost_cache_read_micros = Some(0);
    storage.append_request_event(&rolled).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let mut unrolled = rolled.clone();
    unrolled.request_id = "req-principal-unrolled".to_owned();
    unrolled.event_id = None;
    unrolled.ts_ms = Some((bucket_ts + 1) * 1_000);
    unrolled.cost_usd_micros = Some(5);
    unrolled.cost_input_micros = Some(5);
    storage.append_request_event(&unrolled).await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=principal",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let bucket = body["series"][0]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["virtual_cost_micros"] == 10)
        .expect("rolled bucket");
    let bucket = bucket.as_object().unwrap();
    assert!(!bucket.contains_key("cost_input_micros"));
    assert!(!bucket.contains_key("cost_output_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_5m_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_1h_micros"));
    assert!(!bucket.contains_key("cost_cache_read_micros"));
}

#[tokio::test]
async fn t3__usage_non_principal_grouping_does_not_expose_component_costs() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(6);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);
    let mut event = usage_event(
        bucket_ts,
        "req-model-components",
        upstream_id,
        "target-upstream",
        1,
    );
    event.cost_usd_micros = Some(1);
    event.cost_input_micros = Some(1);
    event.cost_output_micros = Some(0);
    event.cost_cache_creation_5m_micros = Some(0);
    event.cost_cache_creation_1h_micros = Some(0);
    event.cost_cache_read_micros = Some(0);
    storage.append_request_event(&event).await.unwrap();
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let bucket = body["series"][0]["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["request_count"] == 1)
        .expect("observed bucket")
        .as_object()
        .unwrap();
    assert!(!bucket.contains_key("cost_input_micros"));
    assert!(!bucket.contains_key("cost_output_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_5m_micros"));
    assert!(!bucket.contains_key("cost_cache_creation_1h_micros"));
    assert!(!bucket.contains_key("cost_cache_read_micros"));
}

#[tokio::test]
async fn t3__summary_error_rate_excludes_client_navigation_statuses() {
    let clock = test_clock();
    let (_dir, storage) =
        crate::config_admin_common::sqlite_temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let bucket_ts = current_unix_secs(clock.as_ref()).saturating_sub(60);

    for (index, status) in [200, 401, 403, 404, 500].into_iter().enumerate() {
        let mut event = usage_event(
            bucket_ts,
            &format!("req-status-{status}"),
            upstream_id,
            "target-upstream",
            index as u64,
        );
        event.status = status;
        storage.append_request_event(&event).await.unwrap();
    }
    storage.rollup_usage_once().await.unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/dashboard/summary?range=1h", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["totals"]["request_count"], 5);
    assert_eq!(body["totals"]["error_count"], 1);
    assert_eq!(body["totals"]["error_rate"], 0.2);
}

#[tokio::test]
async fn t2__usage_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(
        app(state),
        "GET",
        "/admin/usage?range=1h&group_by=model",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS))
}

fn current_unix_secs(clock: &dyn cc_lb_clock::Clock) -> u64 {
    cc_lb_clock::unix_secs(clock.now())
}

async fn usage_projection_pair(admin_app: &axum::Router, full_uri: &str) -> (Value, Value) {
    let (full_status, _, full, _) = authed_json(admin_app.clone(), "GET", full_uri, None).await;
    let totals_uri = format!("{full_uri}&projection=totals");
    let (totals_status, _, totals, _) =
        authed_json(admin_app.clone(), "GET", totals_uri.as_str(), None).await;
    assert_eq!(full_status, StatusCode::OK);
    assert_eq!(totals_status, StatusCode::OK);
    (full, totals)
}

fn assert_usage_totals_match_full_collapse(full: &Value, totals: &Value) {
    for field in [
        "range",
        "step",
        "group_by",
        "window_start_unix_secs",
        "window_end_unix_secs",
        "truncated_series_count",
        "observed",
    ] {
        assert_eq!(full.get(field), totals.get(field), "response field {field}");
    }

    let full_series = full["series"].as_array().expect("full series array");
    let totals_series = totals["series"].as_array().expect("totals series array");
    assert_eq!(full_series.len(), totals_series.len());
    for (full_item, totals_item) in full_series.iter().zip(totals_series) {
        assert_eq!(full_item.get("key"), totals_item.get("key"));
        assert_eq!(
            full_item.get("upstream_name"),
            totals_item.get("upstream_name")
        );

        let full_buckets = full_item["buckets"].as_array().expect("full buckets array");
        let totals_buckets = totals_item["buckets"]
            .as_array()
            .expect("totals buckets array");
        assert_eq!(totals_buckets.len(), 1);
        let totals_bucket = &totals_buckets[0];
        assert_eq!(
            totals_bucket["bucket_start_unix_secs"],
            totals["window_start_unix_secs"]
        );

        for field in [
            "request_count",
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
            "error_count",
            "virtual_cost_micros",
            "latency_ms_sum",
            "latency_count",
            "proxy_setup_ms_sum",
            "proxy_setup_ms_count",
            "shape_ms_sum",
            "shape_ms_count",
            "sign_ms_sum",
            "sign_ms_count",
            "upstream_ttfb_ms_sum",
            "upstream_ttfb_ms_count",
            "upstream_body_ms_sum",
            "upstream_body_ms_count",
            "cost_input_micros",
            "cost_output_micros",
            "cost_cache_creation_5m_micros",
            "cost_cache_creation_1h_micros",
            "cost_cache_read_micros",
        ] {
            let expected = full_buckets
                .iter()
                .filter_map(|bucket| bucket.get(field).and_then(Value::as_u64))
                .sum::<u64>();
            let expected = full_buckets
                .iter()
                .any(|bucket| bucket.get(field).is_some())
                .then_some(expected);
            assert_eq!(
                totals_bucket.get(field).and_then(Value::as_u64),
                expected,
                "collapsed field {field} for {}",
                full_item["key"]
            );
        }

        let expected_latency_min = full_buckets
            .iter()
            .filter_map(|bucket| bucket["latency_ms_min"].as_u64())
            .min();
        let expected_latency_max = full_buckets
            .iter()
            .filter_map(|bucket| bucket["latency_ms_max"].as_u64())
            .max();
        assert_eq!(
            totals_bucket["latency_ms_min"].as_u64(),
            expected_latency_min
        );
        assert_eq!(
            totals_bucket["latency_ms_max"].as_u64(),
            expected_latency_max
        );
    }
}

fn assert_usage_bucket_fields(bucket: &Value, expected: &[(&str, u64)]) {
    for (field, expected) in expected {
        assert_eq!(
            bucket.get(field).and_then(Value::as_u64),
            Some(*expected),
            "usage bucket field {field}"
        );
    }
}

fn usage_event(
    ts: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
    input_tokens: u64,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(ts * 1000),
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some("test-key".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(input_tokens),
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
