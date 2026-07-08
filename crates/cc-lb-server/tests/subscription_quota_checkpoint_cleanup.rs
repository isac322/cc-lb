mod subscription_quota_checkpoint_cleanup_support;

use cc_lb_storage_api::MetaStore;
use serde_json::json;

use subscription_quota_checkpoint_cleanup_support::*;

#[tokio::test]
async fn compact_subscription_quota_history_drops_raw_table_after_validated_backfill()
-> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    insert_large_raw_history(&storage).await?;
    insert_latest_row(&storage, &observation(10_000, 1)).await?;
    let size_before = std::fs::metadata(&path)?.len();
    drop(storage);

    let output = run_compact_cleanup(&path)?;

    assert_success(&output);
    let report = output_json(&output)?;
    assert_eq!(report["version"], json!(1));
    assert_eq!(report["backfill"]["validation_outcome"], json!("passed"));
    assert_eq!(report["raw_observation_table_dropped"], json!(true));
    assert_eq!(report["integrity_check"], json!("ok"));
    assert!(report["page_count_after"].as_u64() < report["page_count_before"].as_u64());
    assert!(report["database_size_bytes_after"].as_u64() < Some(size_before));

    let storage = open_sqlite_storage(&path).await?;
    assert_eq!(
        table_exists(&storage, "upstream_subscription_quota_observations_v1").await?,
        0
    );
    assert_eq!(
        table_count(&storage, "upstream_subscription_quota_checkpoints_v1").await?,
        1
    );
    assert_eq!(
        table_count(&storage, "upstream_subscription_quota_latest_v1").await?,
        1
    );
    assert_eq!(integrity_check(&storage).await?, "ok");
    assert!(storage.get_meta_value(BACKFILL_MARKER_KEY).await?.is_some());
    assert!(storage.get_meta_value(CLEANUP_MARKER_KEY).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn compact_subscription_quota_history_cleanup_refuses_validation_mismatch()
-> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    insert_raw_observations(&storage, &[observation(10_000, 1)]).await?;
    drop(storage);

    assert_success(&run_compact_backfill(&path)?);
    let storage = open_sqlite_storage(&path).await?;
    sqlx::query("UPDATE upstream_subscription_quota_checkpoints_v1 SET utilization = 0.99")
        .execute(storage.pool())
        .await?;
    drop(storage);

    let output = run_compact_cleanup(&path)?;

    assert!(!output.status.success(), "mismatch cleanup must fail");
    let storage = open_sqlite_storage(&path).await?;
    assert_eq!(
        table_exists(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    assert_eq!(
        table_count(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    assert!(storage.get_meta_value(CLEANUP_MARKER_KEY).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn compact_subscription_quota_history_cleanup_refuses_malformed_raw_rows()
-> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    let mut row = observation(10_000, 1);
    row.upgrade_paths = None;
    insert_raw_observations(&storage, &[row]).await?;
    sqlx::query(
        "UPDATE upstream_subscription_quota_observations_v1 SET upgrade_paths = 'not-json'",
    )
    .execute(storage.pool())
    .await?;
    drop(storage);

    let output = run_compact_cleanup(&path)?;

    assert!(!output.status.success(), "malformed cleanup must fail");
    let storage = open_sqlite_storage(&path).await?;
    assert_eq!(
        table_exists(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    assert!(storage.get_meta_value(BACKFILL_MARKER_KEY).await?.is_none());
    assert!(storage.get_meta_value(CLEANUP_MARKER_KEY).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn compact_subscription_quota_history_cleanup_marker_write_failure_keeps_raw_table()
-> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    insert_raw_observations(&storage, &[observation(10_000, 1)]).await?;
    block_cleanup_marker_writes(&storage).await?;
    drop(storage);

    let output = run_compact_cleanup(&path)?;

    assert!(!output.status.success(), "blocked marker cleanup must fail");
    let storage = open_sqlite_storage(&path).await?;
    assert_eq!(
        table_exists(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    assert_eq!(
        table_count(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    assert!(storage.get_meta_value(CLEANUP_MARKER_KEY).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn compact_subscription_quota_history_cleanup_refuses_stale_marker_with_raw_table()
-> anyhow::Result<()> {
    let (dir, storage) = open_storage().await?;
    let path = storage_path(dir.path());
    insert_raw_observations(&storage, &[observation(10_000, 1)]).await?;
    storage
        .put_meta_value(CLEANUP_MARKER_KEY, &stale_cleanup_marker_json())
        .await?;
    drop(storage);

    let output = run_compact_cleanup(&path)?;

    assert!(!output.status.success(), "stale marker cleanup must fail");
    let storage = open_sqlite_storage(&path).await?;
    assert_eq!(
        table_exists(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    assert_eq!(
        table_count(&storage, "upstream_subscription_quota_observations_v1").await?,
        1
    );
    Ok(())
}

#[test]
fn compact_subscription_quota_history_cleanup_requires_explicit_storage_path() -> anyhow::Result<()>
{
    let output = run_compact_cleanup_without_storage_path()?;

    assert!(!output.status.success(), "cleanup without path must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--storage-path is required"),
        "stderr should explain explicit path requirement: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
