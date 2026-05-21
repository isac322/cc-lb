mod common;

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test]
async fn fuel_timeout_infinite_loop_stability() {
    let loop_fixture = common::fixture(
        "infinite-loop",
        &common::infinite_loop_module(),
        common::metadata(&[("fuel_max", 20_000), ("max_call_duration_ms", 200)]),
    );
    let good = common::fixture(
        "fuel-good",
        &common::module_with_authn(&common::authn_response("host_alive")),
        common::metadata(&[]),
    );
    let runtime = ExtismRuntime::new();
    let loop_plugin = runtime
        .instantiate(&loop_fixture.manifest)
        .expect("loop plugin instantiates");
    let err = match loop_plugin.authenticate(&common::ctx()).await {
        Ok(_) => panic!("infinite loop unexpectedly succeeded"),
        Err(err) => err,
    };
    println!("fuel_timeout_error={err}");

    let good_plugin = runtime
        .instantiate(&good.manifest)
        .expect("good plugin instantiates");
    let outcome = good_plugin
        .authenticate(&common::ctx())
        .await
        .expect("host remains stable");
    println!("fuel_host_alive={}", outcome.principal.id);
    assert_eq!(outcome.principal.id, "host_alive");
}
