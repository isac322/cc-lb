use std::net::SocketAddr;
use std::path::PathBuf;

use cc_lb_config::{
    AdminAuthProviderConfig, AnthropicOAuthConfig, Config, PostgresPoolConfig, StorageConfig,
    TlsConfig, WasmtimeAllocationStrategy,
};
use cc_lb_server::reload::summarize_restart_required;
use url::Url;

#[test]
fn listener_proxy_addr_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.listener.proxy_addr = socket("127.0.0.1:18080");

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "listener.proxy_addr");
}

#[test]
fn listener_tls_cert_path_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.listener.tls = Some(TlsConfig {
        cert_path: Some(PathBuf::from("/tmp/server.crt")),
        key_path: None,
        reload_on_sighup: true,
    });

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "listener.tls.cert_path");
}

#[test]
fn storage_kind_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.storage = StorageConfig::Postgres {
        url: "postgres://user:pass@localhost/db".to_owned(),
        pool: PostgresPoolConfig::default(),
    };

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "storage.kind");
}

#[test]
fn aead_key_env_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.aead.key_env = "CC_LB_NEXT_MASTER_KEY".to_owned();

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "aead.key_env");
}

#[test]
fn oauth_anthropic_client_id_change_returns_entry() {
    let mut current = Config::default();
    let mut new_config = current.clone();
    current.oauth.anthropic = Some(anthropic_oauth("old-client"));
    new_config.oauth.anthropic = Some(anthropic_oauth("new-client"));

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "oauth.anthropic.client_id");
}

#[test]
fn admin_auth_provider_changes_return_entry_but_unchanged_providers_do_not() {
    let mut current = Config::default();
    current.admin.auth.providers = vec![static_token_provider("primary", "PRIMARY_ADMIN_TOKEN")];

    let unchanged = summarize_restart_required(&current, &current);
    assert!(
        unchanged
            .iter()
            .all(|change| change.field != "admin.auth.providers"),
        "unchanged providers should not require a restart: {unchanged:?}"
    );

    let mut new_config = current.clone();
    new_config.admin.auth.providers.push(static_token_provider(
        "break-glass",
        "BREAK_GLASS_ADMIN_TOKEN",
    ));

    let changes = summarize_restart_required(&current, &new_config);
    let change = changes
        .iter()
        .find(|change| change.field == "admin.auth.providers")
        .expect("admin auth providers restart entry");

    assert_eq!(
        change.current,
        r#"[{"kind":"static_token","id":"primary","token_env":"PRIMARY_ADMIN_TOKEN"}]"#
    );
    assert_eq!(
        change.new,
        r#"[{"kind":"static_token","id":"primary","token_env":"PRIMARY_ADMIN_TOKEN"},{"kind":"static_token","id":"break-glass","token_env":"BREAK_GLASS_ADMIN_TOKEN"}]"#
    );
    assert!(change.reason.contains("built once"));
}

#[test]
fn wasmtime_runtime_knob_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.runtime.wasmtime.memory_reservation_bytes = Some(512 * 1024 * 1024);

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "runtime.wasmtime.memory_reservation_bytes");
}

#[test]
fn wasmtime_allocation_strategy_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.runtime.wasmtime.allocation_strategy = WasmtimeAllocationStrategy::Pooling;

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "runtime.wasmtime.allocation_strategy");
}

#[test]
fn upstream_affinity_ttl_change_returns_exact_restart_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.upstream_affinity.ttl_days = 14;

    let changes = summarize_restart_required(&current, &new_config);
    let change = changes
        .iter()
        .find(|change| change.field == "upstream_affinity.ttl_days")
        .expect("upstream affinity TTL restart entry");

    assert_eq!(change.current, "90");
    assert_eq!(change.new, "14");
    assert!(change.reason.contains("engine and scheduler"));
}

fn socket(value: &str) -> SocketAddr {
    value.parse().expect("valid socket address")
}

fn anthropic_oauth(client_id: &str) -> AnthropicOAuthConfig {
    AnthropicOAuthConfig {
        client_id: client_id.to_owned(),
        auth_url: Url::parse("https://example.test/oauth/authorize").unwrap(),
        token_url: Url::parse("https://example.test/oauth/token").unwrap(),
        redirect_uri: Url::parse("https://example.test/oauth/callback").unwrap(),
        scopes: vec!["messages".to_owned()],
    }
}

fn static_token_provider(id: &str, token_env: &str) -> AdminAuthProviderConfig {
    AdminAuthProviderConfig::StaticToken {
        id: id.to_owned(),
        token_env: token_env.to_owned(),
    }
}

fn assert_field(changes: &[cc_lb_config::RestartRequiredField], field: &str) {
    assert!(
        changes.iter().any(|change| change.field == field),
        "missing {field} in {changes:?}"
    );
}
