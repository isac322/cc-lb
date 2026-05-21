mod common;

use std::sync::Arc;

use cc_lb_plugin_api::sign_request;
use cc_lb_signer_gcp::StaticGcpTokenProvider;
use http::header::AUTHORIZATION;

#[tokio::test]
async fn proactive_refresh() {
    let provider = Arc::new(
        StaticGcpTokenProvider::new(common::token("ya29.expiring", 30))
            .with_refresh_tokens(vec![common::token("ya29.refreshed", 3600)]),
    );
    let signer = common::signer(provider.clone());

    let signed = sign_request(&signer, common::shaped_request())
        .await
        .expect("request signs");

    assert_eq!(provider.refresh_calls(), 1);
    assert_eq!(
        signed
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer ya29.refreshed")
    );
}
