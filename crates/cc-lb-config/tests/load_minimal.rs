mod common;

use std::path::PathBuf;

use cc_lb_config::{
    Config, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES,
    DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH,
    DEFAULT_PLUGIN_BATCHED_FLUSH_MS, DEFAULT_REDB_PATH, StorageConfig,
};

#[test]
fn load_minimal_toml_applies_plan_defaults() {
    let (_dir, path) = common::temp_config("[listener]\n");

    let config = Config::load(&path).unwrap();

    assert_eq!(config.listener.proxy_addr, "[::]:8080".parse().unwrap());
    assert_eq!(config.listener.admin_addr, "[::1]:9090".parse().unwrap());
    assert_eq!(config.listener.metrics_addr, "[::1]:9091".parse().unwrap());
    assert_eq!(config.body.messages_cap_bytes, DEFAULT_MESSAGES_CAP_BYTES);
    assert_eq!(config.body.files_cap_bytes, DEFAULT_FILES_CAP_BYTES);
    assert_eq!(
        config.storage,
        StorageConfig::Redb {
            path: PathBuf::from(DEFAULT_REDB_PATH)
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
}

#[test]
fn plugin_refs_apply_extism_batch_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let wasm = common::write_temp_file(dir.path(), "authn.wasm", b"wasm");
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"[plugins.authn_plugin]
name = "authn"
wasm_path = "{}"
"#,
            common::toml_path(&wasm)
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();
    let plugin = config.plugins.authn_plugin.unwrap();

    assert!(!plugin.sse_per_event);
    assert_eq!(
        plugin.batched_events_per_flush,
        DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH
    );
    assert_eq!(plugin.batched_flush_ms, DEFAULT_PLUGIN_BATCHED_FLUSH_MS);
}
