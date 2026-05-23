use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{BucketKind, QuotaManager, QuotaPolicy, start_sweep};
use cc_lb_storage_redb::RedbStorage;

#[tokio::test]
async fn sweep_task_removes_windows_older_than_retention() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(RedbStorage::open(&dir.path().join("quota.redb"))?);
    storage.incr_quota("old", 1, BucketKind::Requests, 1)?;
    let manager = Arc::new(QuotaManager::new(
        Arc::clone(&storage),
        QuotaPolicy {
            window_secs: 60,
            capacity_requests: 10,
            capacity_input_tokens: 10,
            capacity_output_tokens: 10,
        },
    ));

    let handle = start_sweep(manager, Duration::from_millis(10));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while storage.get_quota("old", 1, BucketKind::Requests)? != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sweep task did not remove old quota window before deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.abort();

    assert_eq!(storage.get_quota("old", 1, BucketKind::Requests)?, 0);
    Ok(())
}
