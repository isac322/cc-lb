use std::net::SocketAddr;
use std::path::PathBuf;

use cc_lb_config::{Config, PostgresPoolConfig, StorageConfig, TlsConfig};
use cc_lb_server::reload::summarize_restart_required;

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
fn subscription_quota_scheduler_snapshot_fields_return_entries() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.subscription_quota.writer_channel_capacity = current
        .subscription_quota
        .writer_channel_capacity
        .wrapping_add(1);
    new_config.subscription_quota.writer_batch_max_records = current
        .subscription_quota
        .writer_batch_max_records
        .wrapping_add(1);
    new_config.subscription_quota.writer_flush_ms =
        current.subscription_quota.writer_flush_ms.wrapping_add(1);
    new_config.subscription_quota.routing_max_staleness_secs = current
        .subscription_quota
        .routing_max_staleness_secs
        .wrapping_add(1);

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "subscription_quota.writer_channel_capacity");
    assert_field(&changes, "subscription_quota.writer_batch_max_records");
    assert_field(&changes, "subscription_quota.writer_flush_ms");
    assert_field(&changes, "subscription_quota.routing_max_staleness_secs");
}

#[test]
fn timeouts_upstream_total_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.timeouts.upstream_total_secs = current.timeouts.upstream_total_secs + 60;

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "timeouts.upstream_total_secs");
}

#[test]
fn circuit_breaker_change_returns_entry() {
    let current = Config::default();
    let mut new_config = current.clone();
    new_config.circuit_breaker.failures_to_open =
        current.circuit_breaker.failures_to_open.wrapping_add(3);

    let changes = summarize_restart_required(&current, &new_config);

    assert_field(&changes, "circuit_breaker.failures_to_open");
}

fn socket(value: &str) -> SocketAddr {
    value.parse().expect("valid socket address")
}

fn assert_field(changes: &[cc_lb_config::RestartRequiredField], field: &str) {
    assert!(
        changes.iter().any(|change| change.field == field),
        "missing {field} in {changes:?}"
    );
}
