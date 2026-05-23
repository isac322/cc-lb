mod storage_support;

use cc_lb_core::{QuotaDecision, QuotaManager, QuotaPolicy};
use storage_support::TestStorage;

#[tokio::test]
async fn exhausted_principal_does_not_affect_another_principal()
-> Result<(), Box<dyn std::error::Error>> {
    let storage = TestStorage::new();
    let manager = QuotaManager::new(
        storage.as_storage(),
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
