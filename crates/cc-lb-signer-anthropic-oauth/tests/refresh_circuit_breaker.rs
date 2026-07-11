use crate::common;

use cc_lb_plugin_api::{RetryDecision, Signer};

#[tokio::test]
async fn circuit_breaker_fails_fast_after_three_failures() {
    let clock = common::test_clock();
    let test_storage = common::storage();
    test_storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-expired",
                "refresh-old",
                common::now_epoch_secs(clock.as_ref()) - 10,
            ),
        )
        .await
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(vec![
        common::failure_response(),
        common::failure_response(),
        common::failure_response(),
        common::success_response("sk-ant-oat01-not-used", None, 600),
    ]);
    let signer = common::signer(
        test_storage.storage.clone(),
        test_storage.aead.clone(),
        http.clone(),
        clock,
    );

    for _ in 0..3 {
        match signer.on_unauthorized(&common::unauthorized_error()).await {
            RetryDecision::Fail => {}
            RetryDecision::Refresh { .. } => panic!("refresh unexpectedly succeeded"),
        }
    }
    assert_eq!(http.call_count(), 3);

    match signer.on_unauthorized(&common::unauthorized_error()).await {
        RetryDecision::Fail => {}
        RetryDecision::Refresh { .. } => panic!("breaker did not fail fast"),
    }
    assert_eq!(http.call_count(), 3);
}
