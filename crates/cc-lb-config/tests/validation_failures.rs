use std::fs;
use std::path::PathBuf;

use cc_lb_config::{Config, ConfigError, ConfigOverrides, StorageConfig};

fn load_config(toml: &str) -> Result<Config, ConfigError> {
    Config::from_toml_str_with_overrides(toml, &ConfigOverrides::default())
        .map(|(config, _warnings)| config)
}

#[test]
fn t3__missing_tls_files_fail_with_field_path() {
    let error = load_config(
        r#"[tls]
cert_path = "/definitely/missing/cert.pem"
"#,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("tls.cert_path: file not found"), "{error}");
}

#[test]
fn encrypted_storage_retains_master_key_env_name() {
    let config = load_config(
        r#"[storage]
storage_path = "credentials.sqlite"
oauth_aead_key_env = "CC_LB_TEST_MASTER_KEY_INVALID"
"#,
    )
    .unwrap();

    assert_eq!(config.aead.key_env, "CC_LB_TEST_MASTER_KEY_INVALID");
    assert_eq!(
        config.storage,
        StorageConfig::Sqlite {
            path: PathBuf::from("credentials.sqlite")
        }
    );
}

#[test]
fn zero_upstream_affinity_ttl_fails_with_field_path() {
    let error = load_config(
        r#"
[listener]

[upstream_affinity]
ttl_days = 0
"#,
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.contains(
            "upstream_affinity.ttl_days: upstream affinity TTL must be greater than zero"
        ),
        "{error}"
    );
}

#[test]
fn upstream_affinity_ttl_days_rejects_values_above_u32() {
    let error = toml::from_str::<Config>(
        r#"
[upstream_affinity]
ttl_days = 4294967296
"#,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("ttl_days"), "{error}");
}

#[test]
fn validation_failures_none_mode_legacy_field() {
    let error = load_config(
        r#"[none_mode]
upstream_credential_ref = "x"

[api_keys]
"#,
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("none_mode.upstream_credential_ref"),
        "{error}"
    );
    assert!(error.contains("principal.allowed_upstreams"), "{error}");
}

#[test]
fn none_mode_loads_without_upstream_credential_ref() {
    let config = load_config(
        r#"[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "anon"
upstream_kind = "anthropic_key"

[api_keys]
"#,
    )
    .unwrap();

    assert!(config.downstream_auth.none_mode.is_some());
}

#[test]
fn admin_auth_cloudflare_access_requires_audience() {
    let error = load_config(
        r#"
[[admin.auth.providers]]
kind = "cloudflare_access"
id = "cf"
team_domain = "https://team.cloudflareaccess.com"
audiences = []
"#,
    )
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("admin.auth.providers[0].audiences"),
        "{error}"
    );
}

#[test]
fn admin_auth_cloudflare_access_requires_valid_header_name() {
    let error = load_config(
        r#"
[[admin.auth.providers]]
kind = "cloudflare_access"
id = "cf"
team_domain = "https://team.cloudflareaccess.com"
audiences = ["admin"]
header = "Cf Access Jwt"
"#,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("admin.auth.providers[0].header"), "{error}");
}

#[test]
fn t1__oauth_client_id_empty_rejected() {
    let error = load_config(
        r#"
[oauth.anthropic]
client_id = "   "
"#,
    )
    .expect_err("blank Anthropic OAuth client_id must be rejected");

    match error {
        ConfigError::Validation(error) => {
            assert_eq!(error.field, "oauth.anthropic.client_id");
            assert_eq!(error.message, "client_id cannot be empty");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn t3__sqlite_path_validation_rejects_readonly_and_missing_parent() {
    let dir = tempfile::tempdir().expect("create isolated storage directory");
    let readonly_path = dir.path().join("readonly.sqlite");
    fs::write(&readonly_path, b"sqlite fixture").expect("create storage file");
    let mut permissions = fs::metadata(&readonly_path)
        .expect("inspect storage file")
        .permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&readonly_path, permissions).expect("mark storage file readonly");

    let readonly_error = load_config(&format!(
        "[storage]\nkind = \"sqlite\"\npath = \"{}\"\n",
        crate::common::toml_path(&readonly_path)
    ))
    .expect_err("readonly SQLite file must be rejected");
    match readonly_error {
        ConfigError::Validation(error) => {
            assert_eq!(error.field, "storage.path");
            assert_eq!(
                error.message,
                format!("file is not writable: {}", readonly_path.display())
            );
        }
        other => panic!("unexpected error: {other}"),
    }

    let missing_parent_path = dir.path().join("missing").join("storage.sqlite");
    let missing_parent_error = load_config(&format!(
        "[storage]\nkind = \"sqlite\"\npath = \"{}\"\n",
        crate::common::toml_path(&missing_parent_path)
    ))
    .expect_err("SQLite path with a missing parent must be rejected");
    match missing_parent_error {
        ConfigError::Validation(error) => {
            assert_eq!(error.field, "storage.path");
            assert_eq!(
                error.message,
                format!(
                    "parent directory does not exist: {}",
                    missing_parent_path
                        .parent()
                        .expect("path has parent")
                        .display()
                )
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}
