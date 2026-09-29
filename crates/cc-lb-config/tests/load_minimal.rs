use cc_lb_config::{
    Config, DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV,
    DEFAULT_SQLITE_PATH, DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS, StorageConfig,
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
    assert_eq!(
        config.upstream_affinity.ttl_days,
        DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS
    );
    assert_eq!(
        config.upstream_affinity.ttl_secs(),
        u64::from(DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS) * 86_400
    );
    assert_eq!(config.circuit_breaker.failures_to_open, 5);
    assert_eq!(config.circuit_breaker.window_secs, 10);
    assert_eq!(config.circuit_breaker.half_open_after_secs, 30);
    assert_eq!(config.bulkhead.max_conns_per_upstream, 50);
    assert_eq!(config.bulkhead.semaphore_per_upstream, 100);
    assert_eq!(config.request_event_retention_days, 90);
    assert_eq!(
        config.price_catalog.cache_path,
        std::path::Path::new("/var/lib/cc-lb/litellm.json")
    );
}

#[test]
fn upstream_affinity_ttl_loads_from_toml() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[listener]

[upstream_affinity]
ttl_days = 14
"#,
    );

    let config = Config::load(&path).unwrap();

    assert_eq!(config.upstream_affinity.ttl_days, 14);
    assert_eq!(config.upstream_affinity.ttl_secs(), 14 * 86_400);
}
