use std::sync::Arc;

use cc_lb_core::{QuotaDecision, QuotaManager, QuotaPolicy};
use cc_lb_storage_redb::Storage;

#[tokio::test]
async fn exhausted_principal_does_not_affect_another_principal()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(&dir.path().join("quota.redb"), [21; 32])?);
    let manager = QuotaManager::new(
        storage,
        QuotaPolicy {
            window_secs: 60,
            capacity_requests: 1,
            capacity_input_tokens: 1_000,
            capacity_output_tokens: 1_000,
        },
    );

    let first_a = manager.try_consume_request("principal-a", 1).await;
    let second_a = manager.try_consume_request("principal-a", 1).await;
    let first_b = manager.try_consume_request("principal-b", 1).await;

    println!("principal_a_first={first_a:?}");
    println!("principal_a_second={second_a:?}");
    println!("principal_b_first={first_b:?}");
    assert!(matches!(first_a, QuotaDecision::Allow { .. }));
    assert!(matches!(second_a, QuotaDecision::Reject { .. }));
    assert!(matches!(first_b, QuotaDecision::Allow { .. }));
    Ok(())
}
