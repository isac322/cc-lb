mod common;

use bytes::Bytes;
use cc_lb_plugin_api::{RetryDecision, Signer, UpstreamError};
use cc_lb_signer_aws::clock_skew_total;
use http::StatusCode;

#[tokio::test]
async fn clock_skew_no_retry() {
    let signer = common::signer();
    let before = clock_skew_total();
    let err = UpstreamError::Unauthorized {
        status: StatusCode::BAD_REQUEST,
        body: Some(Bytes::from_static(
            br#"{"__type":"com.amazonaws.bedrock#RequestTimeTooSkewed","message":"skew"}"#,
        )),
    };

    let decision = signer.on_unauthorized(&err).await;

    assert!(matches!(decision, RetryDecision::Fail));
    assert_eq!(clock_skew_total(), before + 1);
    eprintln!("cc_lb_aws_clock_skew_total incremented to {}", before + 1);
}
