use std::sync::Arc;

use cc_lb_core::{QuotaDecision, QuotaManager, QuotaPolicy};
use cc_lb_storage_redb::Storage;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_request_quota_allows_exact_capacity() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(&dir.path().join("quota.redb"), [18; 32])?);
    let manager = Arc::new(QuotaManager::new(
        storage,
        QuotaPolicy {
            window_secs: 60,
            capacity_requests: 100,
            capacity_input_tokens: 1_000_000,
            capacity_output_tokens: 1_000_000,
        },
    ));
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..1_000 {
        let manager = Arc::clone(&manager);
        tasks.spawn(async move {
            matches!(
                manager.try_consume_request("principal-race", 128).await,
                QuotaDecision::Allow { .. }
            )
        });
    }

    let mut allow = 0_u64;
    let mut reject = 0_u64;
    while let Some(result) = tasks.join_next().await {
        if result? {
            allow += 1;
        } else {
            reject += 1;
        }
    }

    println!("allow={allow}, reject={reject}");
    assert_eq!(allow, 100);
    assert_eq!(reject, 900);
    Ok(())
}
