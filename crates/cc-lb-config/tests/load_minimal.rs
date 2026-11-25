use cc_lb_config::{
    Config, DEFAULT_FILES_CAP_BYTES, DEFAULT_MAX_BUFFERED_RESPONSE_BYTES,
    DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_SQLITE_PATH,
    DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS, DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS,
    DEFAULT_UPSTREAM_CONNECT_SECS, StorageConfig, TlsConfig, UnknownOpenaiReasoningPolicy,
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
        config.body.max_buffered_response_bytes,
        DEFAULT_MAX_BUFFERED_RESPONSE_BYTES
    );
    assert_eq!(
        TlsConfig::default().handshake_timeout_secs,
        DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS
    );
    assert_eq!(
        config.timeouts.upstream_connect_secs,
        DEFAULT_UPSTREAM_CONNECT_SECS
    );
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
    assert_eq!(
        config.upstream_affinity.unknown_openai_reasoning,
        UnknownOpenaiReasoningPolicy::Hybrid
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
fn omitted_connect_timeout_clamps_to_short_upstream_total_timeout() {
    let (_dir, path) = crate::common::temp_config(
        r#"[timeouts]
upstream_total_secs = 10
"#,
    );

    let config = Config::load(&path).expect("short total timeout with omitted connect stays valid");
    assert_eq!(config.timeouts.upstream_total_secs, 10);
    assert_eq!(config.timeouts.upstream_connect_secs, 10);

    let json_config: Config =
        serde_json::from_str(r#"{"timeouts":{"upstream_total_secs":12}}"#).expect("JSON decodes");
    json_config.validate().expect("JSON validates");
    assert_eq!(json_config.timeouts.upstream_connect_secs, 12);

    let (_dir_zero, zero_path) = crate::common::temp_config(
        r#"[timeouts]
upstream_total_secs = 0
"#,
    );
    let zero_config =
        Config::load(&zero_path).expect("zero total timeout with omitted connect stays valid");
    assert_eq!(zero_config.timeouts.upstream_total_secs, 0);
    assert_eq!(zero_config.timeouts.upstream_connect_secs, 1);

    let zero_json: Config =
        serde_json::from_str(r#"{"timeouts":{"upstream_total_secs":0}}"#).expect("JSON decodes");
    zero_json.validate().expect("zero JSON validates");
    assert_eq!(zero_json.timeouts.upstream_connect_secs, 1);
}

#[test]
fn upstream_affinity_ttl_loads_from_toml() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[listener]

[upstream_affinity]
ttl_days = 14
unknown_openai_reasoning = "fail_closed"
"#,
    );

    let config = Config::load(&path).unwrap();

    assert_eq!(config.upstream_affinity.ttl_days, 14);
    assert_eq!(config.upstream_affinity.ttl_secs(), 14 * 86_400);
    assert_eq!(
        config.upstream_affinity.unknown_openai_reasoning,
        UnknownOpenaiReasoningPolicy::FailClosed
    );
}

#[test]
fn anthropic_oauth_partial_table_preserves_default_scopes() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[listener]

[oauth.anthropic]
client_id = "custom-client"
"#,
    );

    let config = Config::load(&path).unwrap();

    let anthropic = config
        .oauth
        .anthropic
        .expect("anthropic oauth config should be present");
    assert_eq!(anthropic.client_id, "custom-client");
    assert_eq!(
        anthropic.scopes,
        vec!["user:profile".to_owned(), "user:inference".to_owned()]
    );
}
