mod common;

use cc_lb_plugin_api::{RetryDecision, Signer};

#[tokio::test]
async fn refresh_failure_returns_fail() {
    let test_storage = common::storage();
    test_storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-expired",
                "refresh-old",
                common::now_epoch_secs() - 10,
            ),
        )
        .await
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(vec![common::failure_response()]);
    let signer = common::signer(
        test_storage.storage.clone(),
        test_storage.aead.clone(),
        http.clone(),
    );

    match signer.on_unauthorized(&common::unauthorized_error()).await {
        RetryDecision::Fail => {}
        RetryDecision::Refresh { .. } => panic!("refresh unexpectedly succeeded"),
    }

    println!("oauth_refresh_failure_count=1");
    assert_eq!(http.call_count(), 1);
}
