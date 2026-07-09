use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use anyhow::Context as _;
use cc_lb_storage_api::{
    BackendKind, MetaStore, SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use cc_lb_storage_sqlite::SqliteStorage;
use serde_json::Value;
use uuid::Uuid;

pub const BACKFILL_MARKER_KEY: &str = "subscription_quota_checkpoint_backfill_v1_complete";
pub const CLEANUP_MARKER_KEY: &str = "subscription_quota_checkpoint_cleanup_v1_complete";

pub async fn open_storage() -> anyhow::Result<(tempfile::TempDir, SqliteStorage)> {
    let dir = tempfile::tempdir()?;
    let path = storage_path(dir.path());
    let storage = open_sqlite_storage(&path).await?;
    Ok((dir, storage))
}

pub fn storage_path(dir: &Path) -> PathBuf {
    dir.join("storage.sqlite")
}

pub async fn open_sqlite_storage(path: &Path) -> anyhow::Result<SqliteStorage> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &database_url,
        Arc::new(cc_lb_engine::clock::TestClock::new_at_secs(1_800_000_000)),
    )
    .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok(storage)
}

pub fn run_compact_backfill(storage_path: &Path) -> anyhow::Result<Output> {
    run_compact(storage_path, &[])
}

pub fn run_compact_cleanup(storage_path: &Path) -> anyhow::Result<Output> {
    run_compact(storage_path, &["--drop-raw-observations"])
}

pub fn run_compact_cleanup_without_storage_path() -> anyhow::Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_cc-lb"))
        .args([
            "compact-subscription-quota-history",
            "--drop-raw-observations",
        ])
        .output()?)
}

fn run_compact(storage_path: &Path, extra_args: &[&str]) -> anyhow::Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cc-lb"));
    command.args([
        "compact-subscription-quota-history",
        "--storage-path",
        &storage_path.display().to_string(),
    ]);
    command.args(extra_args);
    Ok(command.output()?)
}

pub async fn insert_large_raw_history(storage: &SqliteStorage) -> anyhow::Result<()> {
    let rows = (0..160u64)
        .map(|index| observation(10_000 + index, u128::from(index) + 1))
        .collect::<Vec<_>>();
    insert_raw_observations(storage, &rows).await
}

pub async fn insert_raw_observations(
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

pub async fn insert_latest_row(
    storage: &SqliteStorage,
    row: &SubscriptionQuotaObservationRecord,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO upstream_subscription_quota_latest_v1 \
         SELECT * FROM upstream_subscription_quota_observations_v1 \
         WHERE upstream_id = ? AND window = ? AND source = ? \
         ORDER BY observed_at_unix_millis DESC LIMIT 1",
    )
    .bind(row.upstream_id.to_string())
    .bind(row.window.as_str())
    .bind(row.source.as_str())
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub async fn table_exists(storage: &SqliteStorage, table: &str) -> anyhow::Result<i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?")
        .bind(table)
        .fetch_one(storage.pool())
        .await
        .with_context(|| format!("checking table existence for {table}"))
}

pub async fn table_count(storage: &SqliteStorage, table: &str) -> anyhow::Result<i64> {
    let sql = match table {
        "upstream_subscription_quota_observations_v1" => {
            "SELECT COUNT(*) FROM upstream_subscription_quota_observations_v1"
        }
        "upstream_subscription_quota_checkpoints_v1" => {
            "SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1"
        }
        "upstream_subscription_quota_latest_v1" => {
            "SELECT COUNT(*) FROM upstream_subscription_quota_latest_v1"
        }
        value => anyhow::bail!("unsupported test table {value}"),
    };
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(storage.pool())
        .await
        .with_context(|| format!("counting {table}"))
}

pub async fn integrity_check(storage: &SqliteStorage) -> anyhow::Result<String> {
    Ok(sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(storage.pool())
        .await?)
}

pub async fn block_cleanup_marker_writes(storage: &SqliteStorage) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TRIGGER block_cleanup_marker_insert \
         BEFORE INSERT ON meta_v1 \
         WHEN NEW.key = 'subscription_quota_checkpoint_cleanup_v1_complete' \
         BEGIN SELECT RAISE(ABORT, 'blocked cleanup marker insert'); END",
    )
    .execute(storage.pool())
    .await?;
    sqlx::query(
        "CREATE TRIGGER block_cleanup_marker_update \
         BEFORE UPDATE ON meta_v1 \
         WHEN NEW.key = 'subscription_quota_checkpoint_cleanup_v1_complete' \
         BEGIN SELECT RAISE(ABORT, 'blocked cleanup marker update'); END",
    )
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub fn stale_cleanup_marker_json() -> String {
    serde_json::json!({
        "version": 1,
        "completed_at_unix_millis": 1_800_000_000_000_u64,
        "backfill": {
            "version": 1,
            "completed_at_unix_millis": 1_800_000_000_000_u64,
            "scanned_raw_rows": 1,
            "inserted_checkpoint_rows": 1,
            "skipped_duplicate_semantic_rows": 0,
            "malformed_rows": 0,
            "validation_outcome": "passed"
        },
        "raw_observation_rows_dropped": 1,
        "raw_observation_table_dropped": true,
        "checkpoint_rows_preserved": 1,
        "latest_rows_preserved": 0,
        "page_count_before": 1,
        "page_count_after": 1,
        "freelist_count_before": 0,
        "freelist_count_after": 0,
        "database_size_bytes_before": 1,
        "database_size_bytes_after": 1,
        "integrity_check": "ok",
        "validation_outcome": "passed"
    })
    .to_string()
}

pub fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "compact exited {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn output_json(output: &Output) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(&output.stdout)?)
}

pub fn observation(
    observed_at_unix_millis: u64,
    sample_id: u128,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id: Uuid::from_u128(0x7788),
        window: SubscriptionQuotaWindow::FiveHour,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::from_u128(sample_id),
        utilization: Some(0.42),
        status: Some(SubscriptionQuotaStatus::Allowed),
        resets_at_unix_secs: Some(1_800_018_000),
        surpassed_threshold: Some(0.75),
        representative_claim: Some(format!("claim-{sample_id}-{}", "x".repeat(8192))),
        fallback_percentage: Some(0.5),
        fallback_available: Some(true),
        overage_in_use: Some(false),
        overage_period_monthly_utilization: Some(0.2),
        upgrade_paths: Some(vec!["team".to_owned(), "enterprise".to_owned()]),
        disabled_reason: None,
        extra_usage_enabled: Some(true),
        extra_usage_monthly_limit: Some(10.0),
        extra_usage_used_credits: Some(0.42),
        ingested_at_unix_millis: observed_at_unix_millis + 10,
    }
}
