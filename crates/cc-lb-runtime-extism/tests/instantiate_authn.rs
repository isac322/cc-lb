mod common;

use bytes::Bytes;
use cc_lb_plugin_api::{PluginRuntime, Upstream, shape_request};
use cc_lb_runtime_extism::ExtismRuntime;

#[test]
fn instantiate_dialect_wrapper() {
    let wat = common::module_with_functions(&[("shape", &common::shape_response())]);
    let fixture = common::fixture("shape", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let dialect = runtime
        .instantiate_dialect(&fixture.manifest)
        .expect("dialect instantiates");

    let shaped = shape_request(
        dialect.as_ref(),
        &common::ctx(),
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect("dialect shapes request");

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
