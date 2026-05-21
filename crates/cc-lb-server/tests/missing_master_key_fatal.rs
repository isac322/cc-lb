mod preflight_common;

use cc_lb_server::preflight::{self, PreflightError, PreflightOptions};

#[tokio::test]
async fn missing_master_key_fatal() {
    let _guard = preflight_common::EnvGuard::remove("DEFINITELY_NOT_SET_T32");
    let mut config = preflight_common::base_config();
    config.storage.redb_path = Some(std::env::temp_dir().join("cc-lb-t32-missing-key.redb"));
    config.storage.oauth_aead_key_env = "DEFINITELY_NOT_SET_T32".to_owned();

    let error = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap_err();

    assert!(
        matches!(error, PreflightError::MasterKeyMissing(name) if name == "DEFINITELY_NOT_SET_T32")
    );
}
