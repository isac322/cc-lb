mod preflight_common;

use cc_lb_server::preflight::{self, PreflightError, PreflightOptions};

#[tokio::test]
async fn bad_redb_path_fatal() {
    let _guard = preflight_common::EnvGuard::set(
        "CC_LB_TEST_MASTER_KEY_T32",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    let mut config = preflight_common::base_config();
    config.storage.redb_path = Some(std::path::PathBuf::from("/proc/this-cannot-exist/foo.redb"));
    config.storage.oauth_aead_key_env = "CC_LB_TEST_MASTER_KEY_T32".to_owned();

    let error = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap_err();

    assert!(matches!(error, PreflightError::Storage(_)));
}
