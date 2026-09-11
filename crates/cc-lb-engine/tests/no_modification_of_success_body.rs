use crate::common;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_engine::{DispatchError, ErrorNormalizer, LifecycleConfig, UpstreamDispatch};
use cc_lb_upstream::SignedRequest;
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use url::Url;

use common::{
    RecordingHook, TestAuthn, TestRouter, TestState, lifecycle_with_parts, messages_request,
};

#[tokio::test]
async fn t2__lifecycle_does_not_invoke_normalizer_for_success_body() {
    let upstream_body = Bytes::from_static(
        br#"{"type":"message","id":"msg_success","content":[{"type":"text","text":"ok"}]}"#,
    );
    let state = TestState::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(state),
        Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        Arc::new(FixedSuccessDispatch {
            body: upstream_body.clone(),
        }),
        vec![Arc::new(RecordingHook::default())],
        LifecycleConfig::default(),
    )
    .with_error_normalizer(Arc::new(ErrorNormalizer::new()));

    let response = lifecycle
        .handle(messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[]}"#,
        )))
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
