mod common;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_core::{DispatchError, ErrorNormalizer, LifecycleConfig, UpstreamDispatch};
use cc_lb_plugin_api::SignedRequest;
use http::{Response, StatusCode};
use http_body_util::BodyExt;
use url::Url;

use common::{
    RecordingHook, TestAuthn, TestRouter, TestState, lifecycle_with_parts, messages_request,
};

#[tokio::test]
async fn lifecycle_does_not_invoke_normalizer_for_success_body() {
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
    write_success_diff_evidence(&upstream_body, &client_body);
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

fn write_success_diff_evidence(upstream_body: &Bytes, client_body: &Bytes) {
    let diff = if upstream_body == client_body {
        "No differences.\n".as_bytes().to_vec()
    } else {
        format!(
            "upstream={:?}\nclient={:?}\n",
            String::from_utf8_lossy(upstream_body),
            String::from_utf8_lossy(client_body)
        )
        .into_bytes()
    };

    for dir in evidence_dirs() {
        fs::create_dir_all(&dir).expect("evidence directory is created");
        fs::write(dir.join("task-24-success-passthrough.diff"), &diff)
            .expect("success passthrough evidence is written");
    }
}

fn evidence_dirs() -> Vec<PathBuf> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    vec![
        PathBuf::from(std::env::var("OUT_DIR").unwrap_or_else(|_| ".omo/evidence".to_owned())),
        manifest_dir.join("../../.omo/evidence"),
    ]
}
