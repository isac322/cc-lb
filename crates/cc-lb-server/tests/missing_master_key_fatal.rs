mod preflight_common;

use cc_lb_config::StorageConfig;
use cc_lb_server::preflight::{self, PreflightError, PreflightOptions};

#[tokio::test]
async fn missing_master_key_fatal() {
    let _guard = preflight_common::EnvGuard::remove("DEFINITELY_NOT_SET_T32");
    let mut config = preflight_common::base_config();
    config.storage = StorageConfig::Redb {
        path: std::env::temp_dir().join("cc-lb-t32-missing-key.redb"),
    };
    config.aead.key_env = "DEFINITELY_NOT_SET_T32".to_owned();

    let error = preflight::run(&config, PreflightOptions { skip_bind: true })
        .await
        .unwrap_err();

    assert!(
        matches!(error, PreflightError::MasterKeyMissing(name) if name == "DEFINITELY_NOT_SET_T32")
    );
}
