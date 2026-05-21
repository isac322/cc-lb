mod common;

use std::sync::Arc;

use cc_lb_plugin_api::sign_request;
use cc_lb_signer_gcp::StaticGcpTokenProvider;
use http::header::AUTHORIZATION;

#[tokio::test]
async fn single_flight_refresh() {
    let provider = Arc::new(
        StaticGcpTokenProvider::new(common::token("ya29.expiring-concurrent", 30))
            .with_refresh_tokens(vec![common::token("ya29.concurrent-refreshed", 3600)]),
    );
    let signer = Arc::new(common::signer(provider.clone()));
    let mut tasks = Vec::new();

    for _ in 0..50 {
        let signer = signer.clone();
        tasks.push(tokio::spawn(async move {
            sign_request(signer.as_ref(), common::shaped_request())
                .await
                .expect("request signs")
        }));
    }

    for task in tasks {
        let signed = task.await.expect("task joins");
        assert_eq!(
            signed
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer ya29.concurrent-refreshed")
        );
    }

    assert_eq!(provider.refresh_calls(), 1);
}
