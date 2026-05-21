use bytes::Bytes;
use cc_lb_plugin_api::{RetryDecision, Signer, UpstreamError};
use cc_lb_signer_anthropic_key::AnthropicKeySigner;

#[tokio::test]
async fn on_unauthorized_returns_fail() {
    let signer = AnthropicKeySigner::new("sk-ant-test-key");
    let decision = signer
        .on_unauthorized(&UpstreamError::Unauthorized {
            status: http::StatusCode::UNAUTHORIZED,
            body: Some(Bytes::from_static(b"unauthorized")),
        })
        .await;

    assert!(matches!(decision, RetryDecision::Fail));
}
