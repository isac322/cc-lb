use std::{path::Path, sync::Arc};

use cc_lb_server::plan_tier_backfill::{
    BackfillReport, infer_pool_quota_blob_backfill, marker_indicates_complete,
    require_existing_sqlite_storage_path, run_pool_quota_blob_backfill, should_write_marker,
};
use cc_lb_storage_api::{
    BackendKind, MetaStore, PoolQuotaContributorBlob, PoolQuotaHistoryStore,
    PoolQuotaSnapshotRecord, SubscriptionQuotaWindow, TierResolutionSource,
};
use uuid::Uuid;

#[test]
fn infer_single_ratio_when_one_blob_contains_one_entry() {
    let upstream_id = Uuid::from_u128(1);
    let blobs = vec![blob(
        10,
        SubscriptionQuotaWindow::FiveHour,
        &entry(upstream_id, 5.0, "fresh", 11),
    )];
    let inferred = infer_pool_quota_blob_backfill(&blobs, 20_000);
    let intervals = inferred
        .intervals_by_upstream
        .get(&upstream_id)
        .expect("upstream intervals");
    assert_eq!(intervals.len(), 1);
    assert_eq!(intervals[0].tier_key.as_deref(), Some("max_5x"));
    assert_eq!(intervals[0].effective_from_unix_millis, 10_000);
    assert_eq!(intervals[0].effective_to_unix_millis, Some(20_000));
}

#[test]
fn infer_collapses_repeated_ratio_and_splits_on_change() {
    let upstream_id = Uuid::from_u128(2);
    let blobs = vec![
        blob(
            10,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 5.0, "fresh", 11),
        ),
        blob(
            20,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 5.0, "fresh", 21),
        ),
        blob(
            30,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 20.0, "fresh", 31),
        ),
    ];
    let inferred = infer_pool_quota_blob_backfill(&blobs, 40_000);
    let intervals = inferred
        .intervals_by_upstream
        .get(&upstream_id)
        .expect("upstream intervals");
    assert_eq!(intervals.len(), 2);
    assert_eq!(intervals[0].effective_to_unix_millis, Some(30_000));
    assert_eq!(intervals[1].tier_key.as_deref(), Some("max_20x"));
}

#[test]
fn infer_keeps_unknown_between_known_runs() {
    let upstream_id = Uuid::from_u128(3);
    let blobs = vec![
        blob(
            10,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 5.0, "fresh", 11),
        ),
        blob(
            20,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 10.0, "fresh", 21),
        ),
        blob(
            30,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 5.0, "fresh", 31),
        ),
    ];
    let inferred = infer_pool_quota_blob_backfill(&blobs, 40_000);
    let intervals = inferred
        .intervals_by_upstream
        .get(&upstream_id)
        .expect("upstream intervals");
    assert_eq!(intervals.len(), 3);
    assert_eq!(
        intervals[1].resolution_source,
        TierResolutionSource::Unknown
    );
    assert!(intervals[1].tier_key.is_none());
}

#[test]
fn infer_deduplicates_cross_window_by_rank() {
    let upstream_id = Uuid::from_u128(4);
    let blobs = vec![
        blob(
            10,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 5.0, "fresh", 10),
        ),
        blob(
            10,
            SubscriptionQuotaWindow::SevenDay,
            &entry(upstream_id, 20.0, "fresh", 10),
        ),
        blob(
            20,
            SubscriptionQuotaWindow::SevenDay,
            &entry(upstream_id, 1.0, "stale", 20),
        ),
        blob(
            20,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 5.0, "fresh", 20),
        ),
    ];
    let inferred = infer_pool_quota_blob_backfill(&blobs, 30_000);
    let intervals = inferred
        .intervals_by_upstream
        .get(&upstream_id)
        .expect("upstream intervals");
    assert_eq!(intervals[0].tier_key.as_deref(), Some("max_20x"));
    assert_eq!(intervals[1].tier_key.as_deref(), Some("max_5x"));
}

#[test]
fn infer_counts_malformed_entries_and_blobs() {
    let upstream_id = Uuid::from_u128(5);
    let blobs = vec![
        blob(10, SubscriptionQuotaWindow::FiveHour, "not-json"),
        blob(20, SubscriptionQuotaWindow::FiveHour, "{}"),
        blob(
            30,
            SubscriptionQuotaWindow::FiveHour,
            "[{\"upstream_id\":\"bad\",\"ratio\":5.0,\"source\":\"merged\",\"state\":\"fresh\",\"observed_at_unix_millis\":1}]",
        ),
        blob(
            40,
            SubscriptionQuotaWindow::FiveHour,
            &entry(upstream_id, 1.0, "fresh", 40),
        ),
    ];
    let inferred = infer_pool_quota_blob_backfill(&blobs, 50_000);
    assert_eq!(inferred.report.malformed_blobs, 2);
    assert_eq!(inferred.report.malformed_entries, 1);
    assert!(inferred.intervals_by_upstream.contains_key(&upstream_id));
}

#[test]
fn marker_indicates_complete_only_for_valid_v1_marker() {
    let report = BackfillReport::default();
    let valid_marker = serde_json::json!({
        "version": 1,
        "completed_at_unix_millis": 123,
        "report": report,
    })
    .to_string();
    let wrong_version = valid_marker.replace("\"version\":1", "\"version\":2");

    assert!(marker_indicates_complete(Some(&valid_marker)));
    assert!(!marker_indicates_complete(None));
    assert!(!marker_indicates_complete(Some("done")));
    assert!(!marker_indicates_complete(Some("{}")));
    assert!(!marker_indicates_complete(Some(&wrong_version)));
}

#[test]
fn should_write_marker_only_when_report_has_no_malformed_data() {
    let clean = BackfillReport::default();
    let malformed_blob = BackfillReport {
        malformed_blobs: 1,
        ..BackfillReport::default()
    };
    let malformed_entry = BackfillReport {
        malformed_entries: 1,
        ..BackfillReport::default()
    };

    assert!(should_write_marker(&clean));
    assert!(!should_write_marker(&malformed_blob));
    assert!(!should_write_marker(&malformed_entry));
}

#[test]
fn require_existing_sqlite_storage_path_accepts_existing_path() {
    let temp_dir = tempfile::tempdir().expect("sqlite temp dir");
    let path = temp_dir.path().join("storage.sqlite");
    std::fs::write(&path, b"").expect("create temp sqlite path");

    let checked = require_existing_sqlite_storage_path(path.clone())
        .expect("existing sqlite path should pass");

    assert_eq!(checked, path);
}

#[test]
fn require_existing_sqlite_storage_path_rejects_missing_path() {
    let temp_dir = tempfile::tempdir().expect("sqlite temp dir");
    let path = temp_dir.path().join("missing-storage.sqlite");

    let error = require_existing_sqlite_storage_path(path.clone())
        .expect_err("missing sqlite path should fail");

    assert!(matches!(
        error,
        cc_lb_server::plan_tier_backfill::PlanTierBackfillError::MissingSqliteStoragePath { path: missing } if missing == path
    ));
}

#[tokio::test]
async fn backfill_reruns_when_malformed_blob_prevents_completion_marker() {
    let temp_dir = tempfile::tempdir().expect("sqlite temp dir");
    let storage = sqlite_storage(&temp_dir.path().join("storage.sqlite")).await;
    let snapshot = PoolQuotaSnapshotRecord {
        snapshot_at_unix_secs: 10,
        window: SubscriptionQuotaWindow::FiveHour,
        utilization: None,
        weighted_utilization_sum: 0.0,
        capacity_ratio_sum: 0.0,
        eligible_upstreams: 0,
        contributing_upstreams: 0,
        stale_upstreams: 0,
        missing_observation_upstreams: 0,
        missing_metadata_upstreams: 0,
        header_contributing_upstreams: 0,
        api_contributing_upstreams: 0,
        max_observed_at_unix_millis: None,
        contributors_json: Some("not-json".to_owned()),
        computed_at_unix_millis: 10_000,
        policy_version: cc_lb_storage_api::POOL_QUOTA_POLICY_VERSION,
    };
    storage
        .record_pool_quota_snapshots(&[snapshot])
        .await
        .expect("record malformed quota snapshot");

    let first = run_pool_quota_blob_backfill(&storage, 20_000)
        .await
        .expect("first backfill run");
    let second = run_pool_quota_blob_backfill(&storage, 30_000)
        .await
        .expect("second backfill run");

    assert_eq!(first.scanned_blobs, 1);
    assert_eq!(first.malformed_blobs, 1);
    assert_eq!(second.scanned_blobs, 1);
    assert_eq!(second.malformed_blobs, 1);
}

async fn sqlite_storage(path: &Path) -> cc_lb_storage_sqlite::SqliteStorage {
    let database_url = format!("sqlite://{}", path.display());
    let clock: cc_lb_engine::ClockHandle = Arc::new(cc_lb_engine::SystemClock);
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .expect("open sqlite storage");
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .expect("initialize sqlite storage");
    storage
}

fn blob(
    snapshot_at_unix_secs: i64,
    window: SubscriptionQuotaWindow,
    contributors_json: &str,
) -> PoolQuotaContributorBlob {
    PoolQuotaContributorBlob {
        snapshot_at_unix_secs,
        window,
        contributors_json: contributors_json.to_owned(),
    }
}

fn entry(upstream_id: Uuid, ratio: f64, state: &str, observed_at_unix_millis: i64) -> String {
    format!(
        "[{{\"upstream_id\":\"{upstream_id}\",\"ratio\":{ratio},\"source\":\"merged\",\"state\":\"{state}\",\"observed_at_unix_millis\":{observed_at_unix_millis}}}]"
    )
}
