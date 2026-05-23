use std::sync::Arc;

use cc_lb_core::{BucketKind, MockClock, QuotaManager, QuotaPolicy};
use cc_lb_storage_redb::RedbStorage;

#[tokio::test]
async fn reconcile_frees_unused_reserved_output_tokens() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(RedbStorage::open(&dir.path().join("quota.redb"))?);
    let manager = QuotaManager::with_clock(
        Arc::clone(&storage),
        QuotaPolicy {
            window_secs: 60,
            capacity_requests: 1_000,
            capacity_input_tokens: 1_000,
            capacity_output_tokens: 1_000,
        },
        Arc::new(MockClock::new(180)),
    );

    let reservation = manager
        .reserve_output("principal-reserve", 100)
        .await
        .unwrap();
    assert_eq!(
        storage.get_quota(
            "principal-reserve",
            reservation.window_start,
            BucketKind::OutputTokens,
        )?,
        100
    );

    manager.reconcile_output(reservation.clone(), 50).await;
    let final_count = storage.get_quota(
        "principal-reserve",
        reservation.window_start,
        BucketKind::OutputTokens,
    )?;
    println!("reserved=100, actual=50, final_counter={final_count}");
    assert_eq!(final_count, 50);
    Ok(())
}
