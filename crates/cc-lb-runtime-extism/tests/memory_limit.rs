mod common;

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test]
async fn memory_limit_stability() {
    let pressure = common::fixture(
        "memory-pressure",
        &common::memory_pressure_module(),
        common::metadata(&[("memory_max_pages", 2), ("max_call_duration_ms", 500)]),
    );
    let good = common::fixture(
        "memory-good",
        &common::module_with_authn(&common::authn_response("host_alive")),
        common::metadata(&[]),
    );
    let runtime = ExtismRuntime::new();
    let pressure_plugin = runtime
        .instantiate(&pressure.manifest)
        .expect("pressure plugin instantiates");
    let err = match pressure_plugin.authenticate(&common::ctx()).await {
        Ok(_) => panic!("memory pressure unexpectedly succeeded"),
        Err(err) => err,
    };
    println!("memory_limit_error={err}");

    let good_plugin = runtime
        .instantiate(&good.manifest)
        .expect("good plugin instantiates");
    let outcome = good_plugin
        .authenticate(&common::ctx())
        .await
        .expect("host remains stable");
    println!("memory_host_alive={}", outcome.principal.id);
    assert_eq!(outcome.principal.id, "host_alive");
}
