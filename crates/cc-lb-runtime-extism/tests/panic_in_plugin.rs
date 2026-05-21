mod common;

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test]
async fn panic_isolation_host_alive() {
    let panic_fixture = common::fixture(
        "panic-plugin",
        &common::panic_module(),
        common::metadata(&[("max_call_duration_ms", 500)]),
    );
    let good = common::fixture(
        "panic-good",
        &common::module_with_authn(&common::authn_response("host_alive")),
        common::metadata(&[]),
    );
    let runtime = ExtismRuntime::new();
    let panic_plugin = runtime
        .instantiate(&panic_fixture.manifest)
        .expect("panic plugin instantiates");
    let err = match panic_plugin.authenticate(&common::ctx()).await {
        Ok(_) => panic!("guest panic unexpectedly succeeded"),
        Err(err) => err,
    };
    println!("panic_error={err}");

    let good_plugin = runtime
        .instantiate(&good.manifest)
        .expect("good plugin instantiates");
    let outcome = good_plugin
        .authenticate(&common::ctx())
        .await
        .expect("host remains stable");
    println!("host_alive=true principal={}", outcome.principal.id);
    assert_eq!(outcome.principal.id, "host_alive");
}
