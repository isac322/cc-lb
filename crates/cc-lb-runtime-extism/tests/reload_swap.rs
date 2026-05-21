mod common;

use std::sync::Arc;
use std::time::Duration;

use cc_lb_plugin_api::PluginRuntime;
use cc_lb_runtime_extism::ExtismRuntime;

#[tokio::test]
async fn reload_swap_midflight() {
    let fixture = common::fixture(
        "reloadable",
        &common::reload_old_module(&common::authn_response("old")),
        common::metadata(&[("fuel_max", 0), ("max_call_duration_ms", 2_000)]),
    );
    let runtime = Arc::new(ExtismRuntime::new());
    let authn = runtime
        .instantiate(&fixture.manifest)
        .expect("old plugin instantiates");
    let in_flight = {
        let authn = authn.clone();
        tokio::spawn(async move { authn.authenticate(&common::ctx()).await })
    };

    tokio::time::sleep(Duration::from_millis(10)).await;
    common::rewrite_fixture(
        &fixture,
        &common::reload_new_module(&common::authn_response("new")),
    );
    runtime.reload("reloadable").expect("reload succeeds");
    let new_outcome = authn
        .authenticate(&common::ctx())
        .await
        .expect("new call uses new plugin");
    assert_eq!(new_outcome.principal.id, "new");

    let old_outcome = in_flight
        .await
        .expect("task joins")
        .expect("old call completes");
    assert_eq!(old_outcome.principal.id, "old");
    println!(
        "reload_new={} reload_old={}",
        new_outcome.principal.id, old_outcome.principal.id
    );
}
