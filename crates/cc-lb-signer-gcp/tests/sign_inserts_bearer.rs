mod common;

use std::sync::Arc;

use cc_lb_plugin_api::sign_request;
use cc_lb_signer_gcp::StaticGcpTokenProvider;
use http::header::AUTHORIZATION;

#[tokio::test]
async fn sign_inserts_bearer() {
    let provider = Arc::new(StaticGcpTokenProvider::new(common::token(
        "ya29.sign-inserts-bearer",
        3600,
    )));
    let signer = common::signer(provider);

    let signed = sign_request(&signer, common::shaped_request())
        .await
        .expect("request signs");

    assert_eq!(
        signed
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer ya29.sign-inserts-bearer")
    );
}
