mod common;

use std::sync::Arc;
use std::time::Duration;

use cc_lb_plugin_api::{RetryDecision, Signer};
use tokio::sync::Barrier;

#[tokio::test]
async fn concurrent_unauthorized_refreshes_single_flight() {
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
    let http = common::FakeOAuthClient::with_response_delay(
        vec![common::success_response(
            "sk-ant-oat01-refreshed",
            None,
            600,
        )],
        Duration::from_millis(25),
    );
    let signer = common::signer(
        test_storage.storage.clone(),
        test_storage.aead.clone(),
        http.clone(),
        clock,
    );
    let task_count = 50;
    let barrier = Arc::new(Barrier::new(task_count));
    let mut tasks = Vec::new();
    for _ in 0..task_count {
        let signer = signer.clone();
        let barrier = barrier.clone();
        let err = common::unauthorized_error();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            signer.on_unauthorized(&err).await
        }));
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
