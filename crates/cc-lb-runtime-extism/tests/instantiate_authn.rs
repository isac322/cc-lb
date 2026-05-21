mod common;

use bytes::Bytes;
use cc_lb_plugin_api::{
    shape_request, sign_request, DialectError, PluginRuntime, ShapedRequest, ShapedRequestBuilder,
    Upstream, UpstreamDialect,
};
use cc_lb_runtime_extism::ExtismRuntime;
use http::{HeaderMap, StatusCode};
use serde_json::json;
use url::Url;

#[tokio::test]
async fn instantiate_authn_returns_outcome_and_signer() {
    let wat = common::module_with_functions(&[
        ("authenticate", &common::authn_response("principal-extism")),
        ("build_signer", &common::build_signer_response()),
        ("sign", &common::sign_response("signed-key")),
    ]);
    let fixture = common::fixture("authn", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("authn instantiates");

    let outcome = authn
        .authenticate(&common::ctx())
        .await
        .expect("authn succeeds");
    assert_eq!(outcome.principal.id, "principal-extism");
    assert_eq!(outcome.quotas.requests_per_window, 10);

    let signer = outcome
        .signer_factory
        .build(&Upstream::AnthropicDirect)
        .await
        .expect("signer builds");
    let shaped = shape_request(
        &StaticDialect,
        &common::ctx(),
        &Upstream::AnthropicDirect,
        &outcome.principal,
    )
    .expect("shaped request is built");
    let signed = sign_request(signer.as_ref(), shaped)
        .await
        .expect("sign succeeds");
    assert_eq!(signed.headers().get("x-api-key").unwrap(), "signed-key");
}

#[test]
fn instantiate_router_and_dialect_wrappers() {
    let wat = common::module_with_functions(&[
        ("route", &common::route_response()),
        ("shape", &common::shape_response()),
    ]);
    let fixture = common::fixture("router", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let router = runtime
        .instantiate_router(&fixture.manifest)
        .expect("router instantiates");
    let route = router
        .route(&common::ctx(), &common::principal())
        .expect("route succeeds");
    match route.upstream {
        Upstream::CustomAnthropicSpec { ref base_url } => {
            assert_eq!(base_url.as_str(), "http://upstream.test/");
        }
        _ => panic!("unexpected upstream"),
    }
    let shaped = shape_request(
        route.dialect.as_ref(),
        &common::ctx(),
        &route.upstream,
        &common::principal(),
    )
    .expect("route dialect shapes request");
    assert_eq!(shaped.url().as_str(), "http://upstream.test/v1/messages");
    assert_eq!(shaped.body(), &Bytes::from_static(br#"{"shaped":true}"#));
}

#[test]
fn instantiate_observability_batches_by_count() {
    let wat = common::module_with_functions(&[("observe", &common::observe_response())]);
    let fixture = common::fixture(
        "observe",
        &wat,
        common::metadata(&[("observe_batch_count", 2), ("observe_flush_ms", 10_000)]),
    );
    let runtime = ExtismRuntime::new();
    let hook = runtime
        .instantiate_observability(&fixture.manifest)
        .expect("observability hook instantiates");
    hook.observe(cc_lb_plugin_api::ObserveEvent::Chunk {
        batch_index: 0,
        event_count: 1,
        total_bytes: 10,
    })
    .expect("first event is buffered");
    hook.observe(cc_lb_plugin_api::ObserveEvent::Chunk {
        batch_index: 1,
        event_count: 1,
        total_bytes: 20,
    })
    .expect("second event flushes batch");
}

#[tokio::test]
async fn unsupported_envelope_version_returns_runtime_error() {
    let output = json!({"_version": 2}).to_string();
    let wat = common::module_with_authn(&output);
    let fixture = common::fixture("bad-version", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("authn instantiates");
    let err = match authn.authenticate(&common::ctx()).await {
        Ok(_) => panic!("unsupported envelope unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("unsupported plugin envelope version"));
}

#[tokio::test]
async fn storage_is_scoped_by_plugin_name() {
    let put_wat =
        common::storage_put_module(&common::authn_response("writer"), b"shared", b"secret");
    let read_wat = common::storage_read_module(
        &common::authn_response("missing"),
        &common::authn_response("leaked"),
        b"shared",
    );
    let writer_fixture = common::fixture("writer", &put_wat, common::metadata(&[]));
    let reader_fixture = common::fixture("reader", &read_wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let writer = runtime
        .instantiate(&writer_fixture.manifest)
        .expect("writer instantiates");
    let reader = runtime
        .instantiate(&reader_fixture.manifest)
        .expect("reader instantiates");

    writer
        .authenticate(&common::ctx())
        .await
        .expect("writer stores value");
    let outcome = reader
        .authenticate(&common::ctx())
        .await
        .expect("reader authenticates");
    assert_eq!(outcome.principal.id, "missing");
}

#[tokio::test]
async fn storage_replacing_existing_key_counts_key_once() {
    let wat = common::storage_put_module(&common::authn_response("quota"), b"shared", b"data");
    let fixture = common::fixture(
        "quota",
        &wat,
        common::metadata(&[("storage_quota_bytes", 16)]),
    );
    let runtime = ExtismRuntime::new();
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("authn instantiates");

    authn
        .authenticate(&common::ctx())
        .await
        .expect("initial storage write fits quota");
    let outcome = authn
        .authenticate(&common::ctx())
        .await
        .expect("same-size replacement still fits quota");
    assert_eq!(outcome.principal.id, "quota");
}

struct StaticDialect;

impl UpstreamDialect for StaticDialect {
    fn shape(
        &self,
        ctx: &cc_lb_plugin_api::RequestContext,
        _upstream: &Upstream,
        _principal: &cc_lb_plugin_api::Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            Url::parse("http://upstream.test/v1/messages").expect("url parses"),
            ctx.method.clone(),
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}
