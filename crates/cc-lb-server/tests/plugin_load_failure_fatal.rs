mod preflight_common;

use cc_lb_server::preflight::{self, PreflightError, PreflightOptions};

#[tokio::test]
async fn plugin_load_failure_fatal() {
    let _guard = preflight_common::EnvGuard::set(
        "CC_LB_TEST_MASTER_KEY_PREFLIGHT_PLUGIN",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    let mut config = preflight_common::base_config();
    preflight_common::use_temp_redb(
        &mut config,
        "preflight-plugin",
        "CC_LB_TEST_MASTER_KEY_PREFLIGHT_PLUGIN",
    );
    config.plugins.authn_plugin = Some(preflight_common::authn_plugin(
        "missing-authn",
        "/definitely/missing/missing-authn.wasm",
    ));

    let error = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap_err();

    assert!(matches!(error, PreflightError::Plugin(_)));
}
