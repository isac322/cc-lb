use std::net::SocketAddr;
use std::path::PathBuf;

use cc_lb_config::{AnthropicOAuthConfig, Config, PostgresPoolConfig, StorageConfig, TlsConfig};
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

fn assert_field(changes: &[cc_lb_config::RestartRequiredField], field: &str) {
    assert!(
        changes.iter().any(|change| change.field == field),
        "missing {field} in {changes:?}"
    );
}
