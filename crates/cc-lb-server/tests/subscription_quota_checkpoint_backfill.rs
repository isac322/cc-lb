use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use cc_lb_storage_api::{
    BackendKind, MetaStore, SubscriptionQuotaCheckpointRecord, SubscriptionQuotaObservationRecord,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSource, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow,
};
use cc_lb_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use uuid::Uuid;

const MARKER_KEY: &str = "subscription_quota_checkpoint_backfill_v1_complete";

#[tokio::test]
async fn compact_subscription_quota_history_replays_raw_rows_deterministically()
-> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    let rows = quota_fixture_rows();
    insert_raw_observations(&storage, &rows).await?;
    drop(storage);

    let output = run_compact(&path)?;

    assert_success_json(
        &output,
        json!({
            "version": 1,
            "completed_at_unix_millis": output_completed_at(&output)?,
            "scanned_raw_rows": 7,
            "inserted_checkpoint_rows": 5,
            "skipped_duplicate_semantic_rows": 2,
            "malformed_rows": 0,
            "validation_outcome": "passed"
        }),
    );

    let storage = open_sqlite_storage(&path).await?;
    let stored_marker = storage.get_meta_value(MARKER_KEY).await?;
    assert_eq!(
        stored_marker.as_deref(),
        Some(String::from_utf8_lossy(&output.stdout).trim())
    );
    let checkpoints = list_checkpoint_fingerprints(&storage).await?;
    let expected = expected_checkpoints(&rows);
    assert_eq!(checkpoints, checkpoint_fingerprints(&expected));

    let rerun = run_compact(&path)?;
    assert_success_json(&rerun, serde_json::from_slice::<Value>(&output.stdout)?);
    let rerun_checkpoints = list_checkpoint_fingerprints(&storage).await?;
    assert_eq!(rerun_checkpoints, checkpoints);
    Ok(())
}

#[tokio::test]
async fn compact_subscription_quota_history_aborts_on_malformed_raw_rows() -> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    let mut row = observation(
        Uuid::from_u128(0x99),
        1_000,
        1,
        SubscriptionQuotaSource::Header,
        0.4,
    );
    row.upgrade_paths = None;
    insert_raw_observations(&storage, &[row]).await?;
    sqlx::query(
        "UPDATE upstream_subscription_quota_observations_v1 SET upgrade_paths = 'not-json'",
    )
    .execute(storage.pool())
    .await?;
    drop(storage);

    let output = run_compact(&path)?;

    assert!(!output.status.success(), "malformed fixture should fail");
    let storage = open_sqlite_storage(&path).await?;
    let marker = storage.get_meta_value(MARKER_KEY).await?;
    assert!(marker.is_none(), "completion marker must not be written");
    let checkpoints = list_checkpoint_fingerprints(&storage).await?;
    assert!(
        checkpoints.is_empty(),
        "malformed replay must not leave checkpoints"
    );
    Ok(())
}

async fn open_storage() -> anyhow::Result<(tempfile::TempDir, SqliteStorage)> {
    let dir = tempfile::tempdir()?;
    let path = storage_path(dir.path());
    let storage = open_sqlite_storage(&path).await?;
    Ok((dir, storage))
}

fn storage_path(dir: &Path) -> std::path::PathBuf {
    dir.join("storage.sqlite")
}

async fn open_sqlite_storage(path: &Path) -> anyhow::Result<SqliteStorage> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &database_url,
        Arc::new(cc_lb_engine::clock::TestClock::new_at_secs(1_800_000_000)),
    )
    .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

fn run_compact(storage_path: &Path) -> anyhow::Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args([
            "compact-subscription-quota-history",
            "--storage-path",
            &storage_path.display().to_string(),
        ])
        .output()?)
}

async fn insert_raw_observations(
    storage: &SqliteStorage,
    rows: &[SubscriptionQuotaObservationRecord],
) -> anyhow::Result<()> {
    for row in rows {
        sqlx::query(
            "INSERT INTO upstream_subscription_quota_observations_v1 \
             (upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id, \
              utilization, status, resets_at_unix_secs, surpassed_threshold, representative_claim, \
              fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, \
              upgrade_paths, disabled_reason, extra_usage_enabled, extra_usage_monthly_limit, \
              extra_usage_used_credits, ingested_at_unix_millis) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(row.upstream_id.to_string())
        .bind(row.window.as_str())
        .bind(row.source.as_str())
        .bind(row.sample_kind.as_str())
        .bind(i64::try_from(row.observed_at_unix_millis)?)
        .bind(row.sample_id.to_string())
        .bind(row.utilization)
        .bind(row.status.map(SubscriptionQuotaStatus::as_str))
        .bind(row.resets_at_unix_secs.map(i64::try_from).transpose()?)
        .bind(row.surpassed_threshold)
        .bind(&row.representative_claim)
        .bind(row.fallback_percentage)
        .bind(row.fallback_available)
        .bind(row.overage_in_use)
        .bind(row.overage_period_monthly_utilization)
        .bind(row.upgrade_paths.as_ref().map(serde_json::to_string).transpose()?)
        .bind(&row.disabled_reason)
        .bind(row.extra_usage_enabled)
        .bind(row.extra_usage_monthly_limit)
        .bind(row.extra_usage_used_credits)
        .bind(i64::try_from(row.ingested_at_unix_millis)?)
        .execute(storage.pool())
        .await?;
    }
    Ok(())
}

async fn list_checkpoint_fingerprints(storage: &SqliteStorage) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT semantic_fingerprint FROM upstream_subscription_quota_checkpoints_v1 \
         ORDER BY upstream_id ASC, window ASC, source ASC, changed_at_unix_millis ASC, sample_id ASC",
    )
    .fetch_all(storage.pool())
    .await?;
    Ok(rows.into_iter().map(hex::encode).collect())
}

fn assert_success_json(output: &Output, expected: Value) {
    assert!(
        output.status.success(),
        "compact exited {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr should be empty: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    assert_eq!(actual, expected);
}

fn output_completed_at(output: &Output) -> anyhow::Result<u64> {
    let value: Value = serde_json::from_slice(&output.stdout)?;
    Ok(value["completed_at_unix_millis"]
        .as_u64()
        .unwrap_or_default())
}

fn quota_fixture_rows() -> Vec<SubscriptionQuotaObservationRecord> {
    let upstream = Uuid::from_u128(0x4242);
    let mut stable_claim = observation(upstream, 2_000, 2, SubscriptionQuotaSource::Header, 0.10);
    stable_claim.representative_claim = Some("claim-churn".to_owned());
    let mut stable_later = observation(upstream, 50_000, 7, SubscriptionQuotaSource::Header, 0.15);
    stable_later.representative_claim = Some("same-state-later".to_owned());
    vec![
        observation(upstream, 1_000, 1, SubscriptionQuotaSource::Header, 0.10),
        stable_claim,
        observation(upstream, 1_500, 3, SubscriptionQuotaSource::Api, 0.50),
        observation(upstream, 30_000, 4, SubscriptionQuotaSource::Header, 0.20),
        observation(upstream, 30_500, 5, SubscriptionQuotaSource::Header, 0.25),
        observation(upstream, 40_000, 6, SubscriptionQuotaSource::Header, 0.15),
        stable_later,
    ]
}

fn expected_checkpoints(
    rows: &[SubscriptionQuotaObservationRecord],
) -> Vec<SubscriptionQuotaCheckpointRecord> {
    [2usize, 0, 3, 4, 5]
        .into_iter()
        .map(|index| SubscriptionQuotaCheckpointRecord::from(&rows[index]))
        .collect()
}

fn checkpoint_fingerprints(rows: &[SubscriptionQuotaCheckpointRecord]) -> Vec<String> {
    rows.iter()
        .map(|row| hex::encode(row.semantic_fingerprint.as_bytes()))
        .collect()
}

fn observation(
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
    sample_id: u128,
    source: SubscriptionQuotaSource,
    utilization: f64,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id,
        window: SubscriptionQuotaWindow::FiveHour,
        source,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::from_u128(sample_id),
        utilization: Some(utilization),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(1_800_018_000),
        surpassed_threshold: Some(0.75),
        representative_claim: Some(format!("claim-{sample_id}")),
        fallback_percentage: Some(0.5),
        fallback_available: Some(true),
        overage_in_use: Some(false),
        overage_period_monthly_utilization: Some(0.2),
        upgrade_paths: Some(vec!["team".to_owned(), "enterprise".to_owned()]),
        disabled_reason: None,
        extra_usage_enabled: Some(true),
        extra_usage_monthly_limit: Some(10.0),
        extra_usage_used_credits: Some(utilization),
        ingested_at_unix_millis: observed_at_unix_millis + 10,
    }
}
