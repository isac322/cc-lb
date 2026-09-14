use std::path::PathBuf;

use cc_lb_config::{
    Config, ConfigError, ConfigOverrides, PostgresPoolConfig, StorageConfig, validate_postgres_url,
};

fn load_config(toml: &str) -> Result<Config, ConfigError> {
    Config::from_toml_str_with_overrides(toml, &ConfigOverrides::default())
        .map(|(config, _warnings)| config)
}

#[test]
fn test_legacy_flat_storage_parses() {
    let storage_path = PathBuf::from("test.sqlite");
    let config = load_config(
        r#"[storage]
storage_path = "test.sqlite"
oauth_aead_key_env = "MY_KEY"
"#,
    )
    .unwrap();

    assert_eq!(config.storage, StorageConfig::Sqlite { path: storage_path });
    assert_eq!(config.aead.key_env, "MY_KEY");
}

#[test]
fn test_postgres_url_invalid_scheme() {
    let error = validate_postgres_url("mysql://host/db").unwrap_err();

    assert!(
        matches!(error, ConfigError::InvalidPostgresUrl { ref message } if message.contains("expected postgres:// or postgresql:// scheme")),
        "{error}"
    );
}

#[test]
fn test_postgres_url_missing_host() {
    let error = validate_postgres_url("postgres:///db").unwrap_err();

    assert!(
        matches!(error, ConfigError::InvalidPostgresUrl { ref message } if message == "missing host"),
        "{error}"
    );
}

#[test]
fn test_postgres_pool_defaults() {
    let pool = PostgresPoolConfig::default();

    assert_eq!(pool.max_connections, 10);
    assert_eq!(pool.min_connections, 1);
    assert_eq!(pool.acquire_timeout_secs, 5);
    assert_eq!(pool.idle_timeout_secs, 600);
    assert_eq!(pool.max_lifetime_secs, 1800);
    assert_eq!(pool.statement_timeout_secs, 30);
    assert!(pool.test_before_acquire);
    assert_eq!(pool.sslmode, "prefer");
}

#[test]
fn test_tagged_kind_with_legacy_storage_path_is_rejected() {
    let error = load_config(
        r#"[storage]
kind = "postgres"
url = "postgres://localhost/db"
storage_path = "/tmp/leftover.sqlite"
"#,
    )
    .expect_err("conflicting [storage] keys must be rejected");

    let message = format!("{error}");
    assert!(
        message.contains("conflicting [storage] keys")
            && message.contains("storage_path")
            && message.contains("`kind`"),
        "unexpected error message: {message}"
    );
}

#[test]
fn test_tagged_kind_postgres_without_legacy_keys_parses() {
    let config = load_config(
        r#"[storage]
kind = "postgres"
url = "postgres://localhost/db"

[cluster]
instance_url = "http://127.0.0.1:9090"
"#,
    )
    .unwrap();

    assert!(matches!(
        config.storage,
        StorageConfig::Postgres { ref url, .. } if url == "postgres://localhost/db"
    ));
}

#[test]
fn test_statement_timeout_exceeds_request_timeout() {
    let mut config = Config::default();
    let request_timeout = config.timeouts.upstream_total_secs;
    config.storage = StorageConfig::Postgres {
        url: "postgres://localhost/db".to_owned(),
        pool: PostgresPoolConfig {
            statement_timeout_secs: request_timeout,
            ..PostgresPoolConfig::default()
        },
    };

    let error = config.validate().unwrap_err();

    assert!(
        matches!(
            error,
            ConfigError::StatementTimeoutExceedsRequestTimeout { statement, request }
                if statement == request_timeout && request == request_timeout
        ),
        "{error}"
    );
}
