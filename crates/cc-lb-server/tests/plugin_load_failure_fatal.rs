mod preflight_common;

use cc_lb_server::preflight::{self, PreflightError, PreflightOptions};

#[tokio::test]
async fn plugin_load_failure_fatal() {
    let mut config = preflight_common::base_config();
    config.plugins.authn_plugin = Some(preflight_common::authn_plugin(
        "missing-authn",
        "/definitely/missing/missing-authn.wasm",
    ));

    let error = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap_err();

    assert!(matches!(error, PreflightError::Plugin(_)));
}
