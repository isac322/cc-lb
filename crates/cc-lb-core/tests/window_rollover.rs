use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{MockClock, QuotaDecision, QuotaManager, QuotaPolicy};
use cc_lb_storage_redb::Storage;

#[tokio::test]
async fn next_window_allows_after_current_window_is_exhausted(
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::open(&dir.path().join("quota.redb"), [19; 32])?);
    let clock = Arc::new(MockClock::new(120));
    let manager = QuotaManager::with_clock(
        storage,
        QuotaPolicy {
            window_secs: 60,
            capacity_requests: 1,
            capacity_input_tokens: 1_000,
            capacity_output_tokens: 1_000,
        },
        clock.clone(),
    );

    assert!(matches!(
        manager.try_consume_request("principal-window", 1).await,
        QuotaDecision::Allow { .. }
    ));
    assert!(matches!(
        manager.try_consume_request("principal-window", 1).await,
        QuotaDecision::Reject { .. }
    ));

    clock.advance(Duration::from_secs(60));
    assert!(matches!(
        manager.try_consume_request("principal-window", 1).await,
        QuotaDecision::Allow { .. }
    ));
    Ok(())
}
