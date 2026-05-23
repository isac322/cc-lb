mod common;

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use cc_lb_dialect_vertex::VertexDialect;
use cc_lb_plugin_api::{SignerFactory, Upstream, shape_request, sign_request};
use cc_lb_signer_gcp::{GcpOAuthSignerFactory, StaticGcpTokenProvider};
use fake_vertex::{AppConfig, app};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn e2e_with_vertex() {
    let provider = Arc::new(StaticGcpTokenProvider::new(common::token(
        "ya29.test",
        3600,
    )));
    let upstream = Upstream::Vertex {
        project: "p".to_owned(),
        region: "us-central1".to_owned(),
    };
    let ctx = common::request_context(common::messages_body(false), common::anthropic_headers());
    let shaped = shape_request(
        &VertexDialect::default(),
        &ctx,
        &upstream,
        &common::principal(),
    )
    .expect("vertex shape succeeds");
    let signer = GcpOAuthSignerFactory::with_provider(provider)
        .build(&upstream)
        .await
        .expect("signer builds");
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("request signs");

    let response = app(AppConfig::default())
        .oneshot(request_from_signed(signed))
        .await
        .expect("response returned");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["type"], "message");
    assert_eq!(
        body["content"][0]["text"],
        "fake vertex fixture response HELLO"
    );
    println!(
        "gcp_vertex_e2e status=200 anthropic_type={} model={}",
        body["type"].as_str().unwrap_or("missing"),
        body["model"].as_str().unwrap_or("missing")
    );
}

fn request_from_signed(signed: cc_lb_plugin_api::SignedRequest) -> Request<Body> {
    let (url, method, headers, body) = signed.into_parts();
    let mut builder = Request::builder().method(method).uri(url.path());
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(body)).expect("request builds")
}
