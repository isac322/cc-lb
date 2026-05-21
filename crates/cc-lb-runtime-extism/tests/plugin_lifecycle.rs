mod common;

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test]
async fn plugin_lifecycle_load_invoke_reload_drop() {
    let fixture = common::fixture(
        "lifecycle",
        &common::module_with_authn(&common::authn_response("before")),
        common::metadata(&[]),
    );
    let runtime = ExtismRuntime::new();
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("load succeeds");
    println!("load_ok");
    let before = authn
        .authenticate(&common::ctx())
        .await
        .expect("invoke succeeds");
    assert_eq!(before.principal.id, "before");
    println!("invoke_ok");

    common::rewrite_fixture(
        &fixture,
        &common::module_with_authn(&common::authn_response("after")),
    );
    runtime.reload("lifecycle").expect("reload succeeds");
    println!("reload_ok");
    let after = authn
        .authenticate(&common::ctx())
        .await
        .expect("invoke after reload succeeds");
    assert_eq!(after.principal.id, "after");
    drop(authn);
    println!("drop_ok");
}
