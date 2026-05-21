mod common;

use cc_lb_plugin_api::{RetryDecision, Signer};

#[tokio::test]
async fn concurrent_unauthorized_refreshes_single_flight() {
    let test_storage = common::storage();
    test_storage
        .storage
        .put_oauth(
            "alice",
            "anthropic_oauth",
            &common::creds(
                "sk-ant-oat01-expired",
                "refresh-old",
                common::now_epoch_secs() - 10,
            ),
        )
        .expect("seed oauth credentials");
    let http = common::FakeOAuthClient::new(vec![common::success_response(
        "sk-ant-oat01-refreshed",
        None,
        600,
    )]);
    let signer = common::signer(test_storage.storage.clone(), http.clone());
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let signer = signer.clone();
        let err = common::unauthorized_error();
        tasks.push(tokio::spawn(
            async move { signer.on_unauthorized(&err).await },
        ));
    }

    for task in tasks {
        match task.await.expect("refresh task joins") {
            RetryDecision::Refresh { .. } => {}
            RetryDecision::Fail => panic!("refresh failed"),
        }
    }

    println!("token_endpoint_call_count={}", http.call_count());
    assert_eq!(http.call_count(), 1);
}
