use crate::common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, LifecycleConfig, UpstreamDispatch};
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};
use http_body_util::BodyExt;

use common::{TestAuthn, TestState, lifecycle_with_parts, messages_request};

#[tokio::test]
async fn lifecycle_does_not_invoke_normalizer_for_success_body() {
    let upstream_body = Bytes::from_static(
        br#"{"type":"message","id":"msg_success","content":[{"type":"text","text":"ok"}]}"#,
    );
    let state = TestState::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state),
        Arc::new(FixedSuccessDispatch {
            body: upstream_body.clone(),
        }),
        LifecycleConfig::default(),
    );

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("lifecycle handles success request");
    assert_eq!(response.status(), StatusCode::OK);

    let client_body = response
        .into_body()
        .collect()
        .await
        .expect("success body collects")
        .to_bytes();
    assert_eq!(client_body, upstream_body);
    // assert_eq! above already reports both bodies on mismatch; no artifact needed.
}

struct FixedSuccessDispatch {
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for FixedSuccessDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}
