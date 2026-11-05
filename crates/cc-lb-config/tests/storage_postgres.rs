use cc_lb_config::{Config, ConfigError, PostgresPoolConfig, StorageConfig, validate_postgres_url};

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
fn test_postgres_pool_omitted_sslmode_stays_omitted_when_serialized() {
    let config = Config::from_stored_toml(
        r#"[storage]
kind = "postgres"
url = "postgres://localhost/db"
"#,
    )
    .unwrap();

    let json = serde_json::to_value(&config).unwrap();
    let pool = json["storage"]["pool"]
        .as_object()
        .expect("serialized postgres pool");
    assert!(
        !pool.contains_key("sslmode"),
        "omitted sslmode must not materialize in JSON: {json}"
    );

    let serialized_toml = toml::to_string_pretty(&config).unwrap();
    let reparsed_toml: toml::Value = toml::from_str(&serialized_toml).unwrap();
    let pool = reparsed_toml["storage"]["pool"]
        .as_table()
        .expect("serialized postgres pool");
    assert!(
        !pool.contains_key("sslmode"),
        "omitted sslmode must not materialize in TOML: {serialized_toml}"
    );

    let reparsed = Config::from_stored_toml(&serialized_toml).unwrap();
    let reparsed_json = serde_json::to_value(reparsed).unwrap();
    assert!(
        !reparsed_json["storage"]["pool"]
            .as_object()
            .unwrap()
            .contains_key("sslmode")
    );
}

#[test]
fn test_postgres_pool_explicit_sslmode_values_survive_roundtrip() {
    for sslmode in [
        "disable",
        "prefer",
        "require",
        "verify-ca",
        "verify_ca",
        "verify-full",
        "verify_full",
    ] {
        let raw_toml = format!(
            r#"[storage]
kind = "postgres"
url = "postgres://localhost/db"

[storage.pool]
sslmode = "{sslmode}"
"#
        );
        let config = Config::from_stored_toml(&raw_toml).unwrap();
        let serialized = toml::to_string_pretty(&config).unwrap();
        let reparsed = Config::from_stored_toml(&serialized).unwrap();
        let json = serde_json::to_value(reparsed).unwrap();

        assert_eq!(json["storage"]["pool"]["sslmode"], sslmode);
    }
}

#[test]
fn test_postgres_pool_defaults_are_usable() {
    let pool = PostgresPoolConfig::default();

    assert_eq!(pool.max_connections, 10);
    assert_eq!(pool.min_connections, 1);
    assert_eq!(pool.acquire_timeout_secs, 5);
    assert_eq!(pool.idle_timeout_secs, 600);
    assert_eq!(pool.max_lifetime_secs, 1800);
    assert_eq!(pool.statement_timeout_secs, 30);
    assert!(pool.test_before_acquire);
}

#[test]
fn test_tagged_postgres_parses() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"[storage]
kind = "postgres"
url = "postgres://localhost/db"

[cluster]
instance_url = "http://127.0.0.1:9090"
"#,
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();

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
