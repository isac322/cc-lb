use cc_lb_storage_redb::{BucketKind, RedbStorage};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_quota_increments_are_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let storage = RedbStorage::open(&path)?;
    let mut join_set = tokio::task::JoinSet::new();

    for _ in 0..100 {
        let storage = storage.clone();
        join_set.spawn_blocking(move || {
            storage.incr_quota("alice", 1_765_000_000, BucketKind::Requests, 1)
        });
    }

    while let Some(result) = join_set.join_next().await {
        result??;
    }

    let final_count = storage.get_quota("alice", 1_765_000_000, BucketKind::Requests)?;
    println!("final counter equals SUM(increments): {final_count}");
    assert_eq!(final_count, 100);

    Ok(())
}
