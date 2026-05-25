use cc_lb_storage_redb::{BucketKind, RedbStorage};

#[test]
fn sweep_old_quotas_removes_only_older_windows() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = RedbStorage::open(&path)?;

    storage.incr_quota("alice", 10, BucketKind::Requests, 3)?;
    storage.incr_quota("alice", 20, BucketKind::InputTokens, 5)?;
    storage.incr_quota("alice", 30, BucketKind::OutputTokens, 7)?;
    storage.incr_quota("bob", 30, BucketKind::Requests, 11)?;

    let deleted = storage.sweep_old_quotas(30)?;
    assert_eq!(deleted, 2);
    assert_eq!(storage.get_quota("alice", 10, BucketKind::Requests)?, 0);
    assert_eq!(storage.get_quota("alice", 20, BucketKind::InputTokens)?, 0);
    assert_eq!(storage.get_quota("alice", 30, BucketKind::OutputTokens)?, 7);
    assert_eq!(storage.get_quota("bob", 30, BucketKind::Requests)?, 11);

    Ok(())
}
