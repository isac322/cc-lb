use std::process::Command;

use cc_lb_config::{
    Config, ConfigError, ConfigOverrides, ListenerOverrides, StorageConfig, ValidationError,
};

type LoadResult = Result<(Config, Vec<String>), ConfigError>;

#[test]
fn t3__valid_config_has_file_and_string_parity() {
    let toml = r#"
[listener]
proxy_addr = "[::1]:7001"
admin_addr = "[::1]:7002"
metrics_addr = "[::1]:7003"

[body]
messages_cap_bytes = 123456
files_cap_bytes = 654321

[upstream_affinity]
ttl_days = 14
"#;

    let (config, warnings) = assert_success_parity(load_pair(toml, ConfigOverrides::default()));

    assert_eq!(config.listener.proxy_addr, "[::1]:7001".parse().unwrap());
    assert_eq!(config.body.messages_cap_bytes, 123_456);
    assert_eq!(config.body.files_cap_bytes, 654_321);
    assert_eq!(config.upstream_affinity.ttl_days, 14);
    assert!(warnings.is_empty());
}

#[test]
fn t3__removed_switch_warnings_have_file_and_string_parity() {
    let toml = r#"
[listener]

[prompt_cache_shadow]
enabled = false
grace_margin_secs = 17

[lifecycle_prompt_cache_drift_subscriber]
enabled = false

[lifecycle_prompt_cache_observation_subscriber]
enabled = false
"#;

    let (config, warnings) = assert_success_parity(load_pair(toml, ConfigOverrides::default()));

    assert_eq!(config.prompt_cache_shadow.grace_margin_secs, 17);
    assert_eq!(
        &warnings[..3],
        [
            "prompt_cache_shadow.enabled",
            "lifecycle_prompt_cache_drift_subscriber.enabled",
            "lifecycle_prompt_cache_observation_subscriber.enabled",
        ]
    );
}

#[test]
fn t3__legacy_storage_migration_has_file_and_string_parity() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_path = storage_dir.path().join("legacy.sqlite");
    let toml = format!(
        r#"
[storage]
storage_path = "{}"
oauth_aead_key_env = "LEGACY_AEAD_KEY"
"#,
        crate::common::toml_path(&storage_path)
    );

    let (config, warnings) = assert_success_parity(load_pair(&toml, ConfigOverrides::default()));

    assert_eq!(config.storage, StorageConfig::Sqlite { path: storage_path });
    assert_eq!(config.aead.key_env, "LEGACY_AEAD_KEY");
    assert!(warnings.is_empty());
}

#[test]
fn t3__invalid_raw_toml_has_file_and_string_error_parity() {
    let toml = r#"
[plugins]
authn_plugin = "removed"
"#;
    let (file_result, string_result) = load_pair(toml, ConfigOverrides::default());

    let file_error = validation_error(file_result.unwrap_err());
    let string_error = validation_error(string_result.unwrap_err());

    assert_eq!(file_error, string_error);
    assert_eq!(file_error.field, "config");
    assert!(file_error.message.contains("v2 removed"));

    let malformed_toml = "[listener\nproxy_addr = \"[::1]:7301\"\n";
    let (file_result, string_result) = load_pair(malformed_toml, ConfigOverrides::default());
    let file_error = file_result.unwrap_err();
    let string_error = string_result.unwrap_err();

    assert!(matches!(file_error, ConfigError::Figment(_)));
    assert!(matches!(string_error, ConfigError::Figment(_)));
    assert_eq!(file_error.to_string(), string_error.to_string());
}

#[test]
fn t3__environment_override_precedence_has_file_and_string_parity() {
    let toml = "[listener]\nproxy_addr = \"[::1]:7101\"\n";
    let (config, _) = assert_success_parity(load_pair(toml, ConfigOverrides::default()));
    let environment_value = "[::1]:7102".parse().unwrap();
    if config.listener.proxy_addr == environment_value {
        return;
    }

    assert_eq!(config.listener.proxy_addr, "[::1]:7101".parse().unwrap());
    run_child_with_env(
        "string_parity::t3__environment_override_precedence_has_file_and_string_parity",
        "CC_LB_LISTENER__PROXY_ADDR",
        "[::1]:7102",
    );
}

#[test]
fn t3__cli_override_precedence_has_file_and_string_parity() {
    let toml = "[listener]\nproxy_addr = \"[::1]:7201\"\n";
    let (environment_config, _) =
        assert_success_parity(load_pair(toml, ConfigOverrides::default()));
    if environment_config.listener.proxy_addr == "[::1]:7202".parse().unwrap() {
        let overrides = ConfigOverrides::from_listener(ListenerOverrides {
            proxy_addr: Some("[::1]:7203".parse().unwrap()),
            ..ListenerOverrides::default()
        });
        let (config, _) = assert_success_parity(load_pair(toml, overrides));
        assert_eq!(config.listener.proxy_addr, "[::1]:7203".parse().unwrap());
        return;
    }

    assert_eq!(
        environment_config.listener.proxy_addr,
        "[::1]:7201".parse().unwrap()
    );
    run_child_with_env(
        "string_parity::t3__cli_override_precedence_has_file_and_string_parity",
        "CC_LB_LISTENER__PROXY_ADDR",
        "[::1]:7202",
    );
}

#[test]
fn t3__missing_file_still_reports_the_exact_toml_file_provider() {
    let dir = tempfile::tempdir().unwrap();
    let missing_path = dir.path().join("missing.toml");

    let error = Config::load_with_overrides_and_warnings(&missing_path, ConfigOverrides::default())
        .unwrap_err();

    let ConfigError::Figment(error) = error else {
        panic!("missing file must remain a Figment error: {error}");
    };
    let metadata = error.metadata.expect("missing file provider metadata");
    assert_eq!(metadata.name, "TOML file");
    assert_eq!(
        metadata
            .source
            .as_ref()
            .and_then(|source| source.file_path()),
        Some(missing_path.as_path())
    );
}

fn load_pair(toml: &str, overrides: ConfigOverrides) -> (LoadResult, LoadResult) {
    let (_dir, path) = crate::common::temp_config(toml);
    let file_result = Config::load_with_overrides_and_warnings(&path, overrides.clone());
    let string_result = Config::from_toml_str_with_overrides(toml, &overrides);
    (file_result, string_result)
}

fn assert_success_parity(results: (LoadResult, LoadResult)) -> (Config, Vec<String>) {
    let (file_result, string_result) = results;
    let (file_config, file_warnings) = file_result.unwrap();
    let (string_config, string_warnings) = string_result.unwrap();

    assert_eq!(
        serde_json::to_value(&file_config).unwrap(),
        serde_json::to_value(&string_config).unwrap()
    );
    assert_eq!(file_config.admin.token, string_config.admin.token);
    assert_eq!(file_warnings, string_warnings);

    (string_config, string_warnings)
}

fn validation_error(error: ConfigError) -> ValidationError {
    match error {
        ConfigError::Validation(error) => error,
        other => panic!("expected validation error, got {other}"),
    }
}

fn run_child_with_env(test_name: &str, key: &str, value: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .env(key, value)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
