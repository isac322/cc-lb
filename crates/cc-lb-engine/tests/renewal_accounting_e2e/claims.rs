use std::sync::Arc;

use cc_lb_storage_api::CacheKeepaliveSessionStore;

pub(crate) async fn concurrent_sqlite_claims(
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    generation: u64,
) -> u8 {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first = tokio::spawn(claim_after_barrier(
        Arc::clone(&storage),
        Arc::clone(&barrier),
        generation,
    ));
    let second = tokio::spawn(claim_after_barrier(storage, barrier, generation));
    let (first, second) = tokio::join!(first, second);
    u8::from(first.expect("first claim task").expect("first claim"))
        + u8::from(second.expect("second claim task").expect("second claim"))
}

async fn claim_after_barrier(
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
    barrier: Arc<tokio::sync::Barrier>,
    generation: u64,
) -> cc_lb_storage_api::StorageResult<bool> {
    barrier.wait().await;
    storage
        .claim_cache_keepalive_turn("renewal-session", generation, 1_002)
        .await
}
