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

/// An operator upgrading across this release still has an `[oauth.anthropic]`
/// section written before long-lived mode existed. `Config` uses
/// `deny_unknown_fields`, so the new key must be optional or startup breaks
/// on every existing deployment.
#[test]
fn pre_upgrade_oauth_toml_without_long_lived_scopes_loads_with_defaults() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[listener]

[oauth.anthropic]
client_id = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
auth_url = "https://claude.ai/oauth/authorize"
token_url = "https://console.anthropic.com/v1/oauth/token"
redirect_uri = "https://console.anthropic.com/oauth/code/callback"
scopes = ["org:create_api_key", "user:profile", "user:inference"]
"#,
    );

    let config = Config::load(&path).expect("pre-upgrade config still loads");

    let anthropic = config.oauth.anthropic.expect("anthropic oauth configured");
    // The operator's explicit refreshing scopes are preserved verbatim.
    assert_eq!(
        anthropic.scopes,
        vec![
            "org:create_api_key".to_owned(),
            "user:profile".to_owned(),
            "user:inference".to_owned(),
        ]
    );
    // The long-lived preset is filled in, and never inherits `org:create_api_key`.
    assert_eq!(
        anthropic.long_lived_scopes,
        vec!["user:profile".to_owned(), "user:inference".to_owned()]
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
