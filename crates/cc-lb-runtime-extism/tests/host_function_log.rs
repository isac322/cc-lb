mod common;

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test]
async fn host_function_log() {
    let wat = common::host_log_module(&common::authn_response("logged"));
    let fixture = common::fixture("host-log", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("authn instantiates");
    let outcome = authn
        .authenticate(&common::ctx())
        .await
        .expect("log host function succeeds");
    println!("host_function_log=hello principal={}", outcome.principal.id);
    assert_eq!(outcome.principal.id, "logged");
}
