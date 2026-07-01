//! BootEnv + Config::compose: env vars and JSON overlay produce the stitched Config.
//!
//! These tests pin down the contract for the new env-only boot pipeline.

use std::path::{Path, PathBuf};

use cc_lb_config::{BootEnv, BootEnvError, Config, DEFAULT_ADMIN_TOKEN_ENV};
use serde_json::json;

/// Helper: run a closure with a clean env (every CC_LB_* variable cleared, then user-supplied set).
fn with_env<F: FnOnce() -> R, R>(vars: &[(&str, &str)], f: F) -> R {
    use std::sync::Mutex;
    static LOCK: Mutex<()> = Mutex::new(());
    let guard = LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // Snapshot existing CC_LB_* env so we can restore them.
    let existing: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("CC_LB_") || k == "OTHER_ADMIN_TOKEN")
        .collect();
    for (k, _) in &existing {
        // SAFETY: tests are gated by a process-local mutex
        unsafe {
            std::env::remove_var(k);
        }
    }
    for (k, v) in vars {
        // SAFETY: tests are gated by a process-local mutex
        unsafe {
            std::env::set_var(k, v);
        }
    }
    let result = f();
    // Restore: first clear what we set, then restore originals.
    for (k, _) in vars {
        unsafe {
            std::env::remove_var(k);
        }
    }
    for (k, v) in existing {
        unsafe {
            std::env::set_var(k, v);
        }
    }
    drop(guard);
    result
}

#[test]
fn boot_env_from_env_reads_required_listener_addrs() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            assert_eq!(boot.listener.proxy_addr.to_string(), "127.0.0.1:18080");
            assert_eq!(boot.listener.admin_addr.to_string(), "127.0.0.1:18081");
            assert_eq!(boot.listener.metrics_addr.to_string(), "127.0.0.1:19090");
            assert_eq!(boot.data_dir, PathBuf::from("/tmp/test-data"));
        },
    );
}

#[test]
fn boot_env_defaults_admin_token_env_when_unset() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            assert_eq!(boot.admin_token_env, DEFAULT_ADMIN_TOKEN_ENV);
        },
    );
}

#[test]
fn boot_env_honors_explicit_admin_token_env() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
            ("CC_LB_ADMIN__TOKEN_ENV", "OTHER_ADMIN_TOKEN"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            assert_eq!(boot.admin_token_env, "OTHER_ADMIN_TOKEN");
        },
    );
}

#[test]
fn boot_env_missing_proxy_addr_errors_clearly() {
    with_env(
        &[
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let err = BootEnv::from_env().expect_err("proxy_addr missing should error");
            let message = err.to_string();
            assert!(
                message.contains("proxy_addr") || message.contains("PROXY_ADDR"),
                "error should mention proxy_addr: {message}"
            );
        },
    );
}

#[test]
fn boot_env_postgres_storage_parses_url() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "postgres"),
            (
                "CC_LB_STORAGE__URL",
                "postgresql://user:pass@localhost:5432/db?sslmode=disable",
            ),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            match &boot.storage {
                cc_lb_config::StorageConfig::Postgres { url, .. } => {
                    assert!(url.contains("localhost:5432"), "url: {url}");
                }
                other => panic!("expected postgres, got {other:?}"),
            }
        },
    );
}

#[test]
fn compose_routes_tls_boot_env_to_listener_tls() {
    let directory = tempfile::tempdir().expect("temporary TLS directory");
    let cert_path = directory.path().join("server.crt");
    let key_path = directory.path().join("server.key");
    std::fs::write(&cert_path, b"certificate").expect("write TLS certificate");
    std::fs::write(&key_path, b"private key").expect("write TLS key");
    let cert_path_env = cert_path.to_string_lossy().into_owned();
    let key_path_env = key_path.to_string_lossy().into_owned();

    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
            ("CC_LB_TLS__CERT_PATH", &cert_path_env),
            ("CC_LB_TLS__KEY_PATH", &key_path_env),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            let config = Config::compose(&boot, None).expect("compose ok");
            let tls = config
                .listener
                .tls
                .expect("listener TLS is set from boot env");

            assert_eq!(tls.cert_path.as_deref(), Some(cert_path.as_path()));
            assert_eq!(tls.key_path.as_deref(), Some(key_path.as_path()));
        },
    );
}

#[test]
fn compose_overlays_runtime_json_onto_defaults() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            let overlay = json!({
                "timeouts": { "upstream_total_secs": 777 },
                "circuit_breaker": { "failures_to_open": 99 },
            });
            let config = Config::compose(&boot, Some(overlay)).expect("compose ok");
            assert_eq!(config.timeouts.upstream_total_secs, 777);
            assert_eq!(config.circuit_breaker.failures_to_open, 99);
        },
    );
}

#[test]
fn compose_boot_fields_override_overlay_attempts() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            // Overlay attempts to overwrite boot fields — must be ignored.
            let overlay = json!({
                "listener": { "proxy_addr": "127.0.0.1:1" },
                "storage": { "kind": "postgres", "url": "postgresql://evil" },
                "admin": { "token_env": "ATTACKER_TOKEN" },
            });
            let config = Config::compose(&boot, Some(overlay)).expect("compose ok");
            assert_eq!(
                config.listener.proxy_addr.to_string(),
                "127.0.0.1:18080",
                "boot listener wins"
            );
            match &config.storage {
                cc_lb_config::StorageConfig::Sqlite { path } => {
                    assert_eq!(path, Path::new("/tmp/test-storage.sqlite"));
                }
                other => panic!("expected sqlite, got {other:?}"),
            }
            assert_eq!(
                config.admin.token_env, DEFAULT_ADMIN_TOKEN_ENV,
                "boot admin.token_env wins"
            );
        },
    );
}

#[test]
fn compose_with_no_overlay_yields_runtime_defaults() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            let config = Config::compose(&boot, None).expect("compose ok");
            // Default messages_cap = 32 MiB.
            assert_eq!(config.body.messages_cap_bytes, 32 * 1024 * 1024);
            // Default circuit breaker.
            assert_eq!(config.circuit_breaker.failures_to_open, 5);
        },
    );
}

#[test]
fn compose_resolves_admin_token_from_env() {
    with_env(
        &[
            ("CC_LB_LISTENER__PROXY_ADDR", "127.0.0.1:18080"),
            ("CC_LB_LISTENER__ADMIN_ADDR", "127.0.0.1:18081"),
            ("CC_LB_LISTENER__METRICS_ADDR", "127.0.0.1:19090"),
            ("CC_LB_STORAGE__KIND", "sqlite"),
            ("CC_LB_STORAGE__PATH", "/tmp/test-storage.sqlite"),
            ("CC_LB_DATA_DIR", "/tmp/test-data"),
            ("CC_LB_ADMIN__TOKEN_ENV", "OTHER_ADMIN_TOKEN"),
            ("OTHER_ADMIN_TOKEN", "the-bearer-token"),
        ],
        || {
            let boot = BootEnv::from_env().expect("boot env parses");
            let config = Config::compose(&boot, None).expect("compose ok");
            assert_eq!(config.admin.token.as_deref(), Some("the-bearer-token"));
        },
    );
}

#[test]
fn runtime_overlay_schema_excludes_boot_only_top_level_keys() {
    let schema = Config::runtime_overlay_schema();
    let schema_value: serde_json::Value =
        serde_json::to_value(&schema).expect("schema serializes to value");
    let props = schema_value
        .pointer("/properties")
        .and_then(|v| v.as_object())
        .expect("schema has properties object");
    for forbidden in ["listener", "tls", "storage", "aead"] {
        assert!(
            !props.contains_key(forbidden),
            "runtime overlay schema must not expose boot-only key `{forbidden}`"
        );
    }
    for required in [
        "body",
        "timeouts",
        "downstream_auth",
        "api_keys",
        "scheduler",
        "observability",
        "oauth",
        "subscription_quota",
        "circuit_breaker",
        "bulkhead",
        "prompt_cache_shadow",
    ] {
        assert!(
            props.contains_key(required),
            "runtime overlay schema must expose runtime key `{required}`"
        );
    }
}

#[allow(dead_code)]
fn _enforce_boot_env_error_is_std_error() {
    fn assert_send_sync<T: std::error::Error + Send + Sync>() {}
    assert_send_sync::<BootEnvError>();
}

#[test]
fn dashboard_view_redacts_postgres_url_credentials() {
    use cc_lb_config::{PostgresPoolConfig, StorageConfig};

    let boot = BootEnv {
        listener: cc_lb_config::ListenerConfig::default(),
        tls: None,
        storage: StorageConfig::Postgres {
            url: "postgresql://alice:hunter2@db.internal:5432/prod".to_owned(),
            pool: PostgresPoolConfig::default(),
        },
        aead_key_env: "CC_LB_MASTER_KEY".to_owned(),
        admin_token_env: "CC_LB_ADMIN_TOKEN".to_owned(),
        data_dir: PathBuf::from("/tmp"),
    };
    let view = boot.dashboard_view();
    let storage_url = view
        .pointer("/storage/url")
        .and_then(|v| v.as_str())
        .expect("storage.url present");
    assert!(
        !storage_url.contains("hunter2"),
        "password leaked: {storage_url}"
    );
    assert!(
        storage_url.contains("***"),
        "redaction marker missing: {storage_url}"
    );
    assert!(
        storage_url.contains("db.internal"),
        "host preserved: {storage_url}"
    );
}
