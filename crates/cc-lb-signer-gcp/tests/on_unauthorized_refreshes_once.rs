mod common;

use std::sync::Arc;

use bytes::Bytes;
use cc_lb_plugin_api::{RetryDecision, Signer, UpstreamError, sign_request};
use cc_lb_signer_gcp::StaticGcpTokenProvider;
use http::StatusCode;
use http::header::AUTHORIZATION;

#[tokio::test]
async fn on_unauthorized_refreshes_once() {
    let provider = Arc::new(
        StaticGcpTokenProvider::new(common::token("ya29.rejected", 3600))
            .with_refresh_tokens(vec![common::token("ya29.unauthorized-refreshed", 3600)]),
    );
    let signer = common::signer(provider.clone());
    let err = UpstreamError::Unauthorized {
        status: StatusCode::UNAUTHORIZED,
        body: Some(Bytes::from_static(br#"{"error":"unauthorized"}"#)),
    };

    let decision = signer.on_unauthorized(&err).await;

    let RetryDecision::Refresh { new_signer } = decision else {
        panic!("expected refreshed signer");
    };
    assert_eq!(provider.refresh_calls(), 1);

    let signed = sign_request(new_signer.as_ref(), common::shaped_request())
        .await
        .expect("request signs");
    assert_eq!(
        signed
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer ya29.unauthorized-refreshed")
    );
}
