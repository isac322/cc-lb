use std::process::Command;

use cc_lb_config::{
    ADMIN_AUTH_PROVIDERS_JSON_ENV, AdminAuthProviderConfig, Config, DEFAULT_ADMIN_TOKEN_ENV,
};

const ENV_OVERRIDE_CHILD: &str = "CC_LB_CONFIG_ENV_OVERRIDE_CHILD";
const ADMIN_AUTH_ENV_CHILD: &str = "CC_LB_CONFIG_ADMIN_AUTH_ENV_CHILD";
const ADMIN_AUTH_INVALID_CHILD: &str = "CC_LB_CONFIG_ADMIN_AUTH_INVALID_CHILD";
const ADMIN_AUTH_LEGACY_CHILD: &str = "CC_LB_CONFIG_ADMIN_AUTH_LEGACY_CHILD";

#[test]
fn double_underscore_env_names_map_to_nested_config_fields() {
    if std::env::var_os(ENV_OVERRIDE_CHILD).is_some() {
        assert_env_override_applies();
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::double_underscore_env_names_map_to_nested_config_fields")
        .env(ENV_OVERRIDE_CHILD, "1")
        .env("CC_LB_LISTENER__PROXY_ADDR", "[::]:9999")
        .env("CC_LB_UPSTREAM_AFFINITY__TTL_DAYS", "21")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_env_override_applies() {
    let (_dir, path) = crate::common::temp_config(
        r#"[listener]
proxy_addr = "[::]:7777"
"#,
    );

    let config = Config::load(&path).unwrap();

    assert_eq!(config.listener.proxy_addr, "[::]:9999".parse().unwrap());
    assert_eq!(config.upstream_affinity.ttl_days, 21);
    assert_eq!(config.upstream_affinity.ttl_secs(), 21 * 86_400);
}

#[test]
fn admin_auth_provider_json_env_overrides_toml_providers() {
    if std::env::var_os(ADMIN_AUTH_ENV_CHILD).is_some() {
        assert_admin_auth_env_override_applies();
        return;
    }

    let providers = r#"[{"kind":"cloudflare_access","id":"cf","team_domain":"https://team.cloudflareaccess.com","audiences":["admin-aud"]}]"#;
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::admin_auth_provider_json_env_overrides_toml_providers")
        .env(ADMIN_AUTH_ENV_CHILD, "1")
        .env(ADMIN_AUTH_PROVIDERS_JSON_ENV, providers)
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

fn assert_admin_auth_env_override_applies() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[[admin.auth.providers]]
kind = "static_token"
id = "toml"
token_env = "TOML_ADMIN_TOKEN"
"#,
    );

    let config = Config::load(&path).unwrap();

    assert_eq!(config.admin.auth.providers.len(), 1);
    assert!(matches!(
        &config.admin.auth.providers[0],
        AdminAuthProviderConfig::CloudflareAccess {
            id,
            team_domain,
            audiences,
            header,
        } if id == "cf"
            && team_domain == "https://team.cloudflareaccess.com"
            && audiences == &["admin-aud"]
            && header == "cf-access-jwt-assertion"
    ));
}

#[test]
fn invalid_admin_auth_provider_json_does_not_fall_back_to_legacy_token() {
    if std::env::var_os(ADMIN_AUTH_INVALID_CHILD).is_some() {
        assert_invalid_admin_auth_env_fails();
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(
            "load_env_override::invalid_admin_auth_provider_json_does_not_fall_back_to_legacy_token",
        )
        .env(ADMIN_AUTH_INVALID_CHILD, "1")
        .env(ADMIN_AUTH_PROVIDERS_JSON_ENV, "not-json")
        .env(DEFAULT_ADMIN_TOKEN_ENV, "legacy-token")
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

fn assert_invalid_admin_auth_env_fails() {
    let (_dir, path) = crate::common::temp_config(
        r#"[admin]
token_env = "CC_LB_ADMIN_TOKEN"
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(error.contains(ADMIN_AUTH_PROVIDERS_JSON_ENV), "{error}");
    assert!(error.contains("invalid"), "{error}");
}

#[test]
fn legacy_admin_token_is_automatically_migrated_in_memory() {
    if std::env::var_os(ADMIN_AUTH_LEGACY_CHILD).is_some() {
        assert_legacy_admin_token_migration();
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::legacy_admin_token_is_automatically_migrated_in_memory")
        .env(ADMIN_AUTH_LEGACY_CHILD, "1")
        .env_remove(ADMIN_AUTH_PROVIDERS_JSON_ENV)
        .env(DEFAULT_ADMIN_TOKEN_ENV, "legacy-token")
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

fn assert_legacy_admin_token_migration() {
    let (_dir, path) = crate::common::temp_config(
        r#"[admin]
token_env = "CC_LB_ADMIN_TOKEN"
"#,
    );

    let (config, warnings) = Config::load_with_warnings(&path).unwrap();

    assert_eq!(config.admin.token.as_deref(), Some("legacy-token"));
    assert!(config.admin.auth.providers.is_empty());
    assert!(
        warnings.iter().any(|warning| {
            warning.contains("static-token/legacy")
                && warning.contains(ADMIN_AUTH_PROVIDERS_JSON_ENV)
        }),
        "{warnings:?}"
    );
}

fn assert_child_succeeded(output: std::process::Output) {
    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
