use cc_lb_config::{
    Config, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES,
    DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_SQLITE_PATH, StorageConfig,
};

#[test]
fn load_minimal_toml_applies_plan_defaults() {
    let (_dir, path) = crate::common::temp_config("[listener]\n");

    let config = Config::load(&path).unwrap();

    assert_eq!(config.listener.proxy_addr, "[::]:8080".parse().unwrap());
    assert_eq!(config.listener.admin_addr, "[::1]:9090".parse().unwrap());
    assert_eq!(config.listener.metrics_addr, "[::1]:9091".parse().unwrap());
    assert_eq!(config.body.messages_cap_bytes, DEFAULT_MESSAGES_CAP_BYTES);
    assert_eq!(config.body.files_cap_bytes, DEFAULT_FILES_CAP_BYTES);
    assert_eq!(
        config.storage,
        StorageConfig::Sqlite {
            path: DEFAULT_SQLITE_PATH.into()
        }
    );
    assert_eq!(config.aead.key_env, DEFAULT_OAUTH_AEAD_KEY_ENV);
    assert_eq!(config.admin.token_env, DEFAULT_ADMIN_TOKEN_ENV);
    assert_eq!(config.circuit_breaker.failures_to_open, 5);
    assert_eq!(config.circuit_breaker.window_secs, 10);
    assert_eq!(config.circuit_breaker.half_open_after_secs, 30);
    assert_eq!(config.bulkhead.max_conns_per_upstream, 50);
    assert_eq!(config.bulkhead.semaphore_per_upstream, 100);
    assert_eq!(config.dns.cache_ttl_floor_secs, 30);
    assert_eq!(config.dns.cache_ttl_ceiling_secs, 300);

    assert!(!config.capture.enabled);
    assert_eq!(
        config.capture.path,
        std::path::PathBuf::from("capture.sqlite")
    );
    assert_eq!(config.capture.channel_capacity, 1024);
    assert_eq!(config.capture.retention_max_rows, 100_000);
}
