use std::process::Command;

use cc_lb_config::{
    ADMIN_AUTH_PROVIDERS_JSON_ENV, AdminAuthProviderConfig, Config, ConfigOverrides,
    DEFAULT_ADMIN_TOKEN_ENV,
};

#[test]
fn double_underscore_env_names_map_to_nested_config_fields() {
    let (config, _warnings) = Config::from_toml_str_with_overrides(
        r#"[listener]
proxy_addr = "[::]:7777"
"#,
        &ConfigOverrides::default(),
    )
    .unwrap();
    if config.listener.proxy_addr == "[::]:9999".parse().unwrap()
        && config.upstream_affinity.ttl_days == 21
    {
        assert_eq!(config.upstream_affinity.ttl_secs(), 21 * 86_400);
        return;
    }

    assert_eq!(config.listener.proxy_addr, "[::]:7777".parse().unwrap());

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::double_underscore_env_names_map_to_nested_config_fields")
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

#[test]
fn admin_auth_provider_json_env_overrides_toml_providers() {
    let (config, _warnings) = Config::from_toml_str_with_overrides(
        r#"
[[admin.auth.providers]]
kind = "static_token"
id = "toml"
token_env = "TOML_ADMIN_TOKEN"
"#,
        &ConfigOverrides::default(),
    )
    .unwrap();

    if matches!(
        &config.admin.auth.providers[..],
        [AdminAuthProviderConfig::CloudflareAccess {
            id,
            team_domain,
            audiences,
            header,
        }] if id == "cf"
            && team_domain == "https://team.cloudflareaccess.com"
            && audiences == &["admin-aud"]
            && header == "cf-access-jwt-assertion"
    ) {
        return;
    }

    let providers = r#"[{"kind":"cloudflare_access","id":"cf","team_domain":"https://team.cloudflareaccess.com","audiences":["admin-aud"]}]"#;
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::admin_auth_provider_json_env_overrides_toml_providers")
        .env(ADMIN_AUTH_PROVIDERS_JSON_ENV, providers)
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

#[test]
fn invalid_admin_auth_provider_json_does_not_fall_back_to_legacy_token() {
    let result = Config::from_toml_str_with_overrides(
        r#"[admin]
token_env = "CC_LB_ADMIN_TOKEN"
"#,
        &ConfigOverrides::default(),
    );

    if let Err(error) = result {
        let error = error.to_string();
        assert!(error.contains(ADMIN_AUTH_PROVIDERS_JSON_ENV), "{error}");
        assert!(error.contains("invalid"), "{error}");
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(
            "load_env_override::invalid_admin_auth_provider_json_does_not_fall_back_to_legacy_token",
        )
        .env(ADMIN_AUTH_PROVIDERS_JSON_ENV, "not-json")
        .env(DEFAULT_ADMIN_TOKEN_ENV, "legacy-token")
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

#[test]
fn legacy_admin_token_is_automatically_migrated_in_memory() {
    let (config, warnings) = Config::from_toml_str_with_overrides(
        r#"[admin]
token_env = "CC_LB_ADMIN_TOKEN"
"#,
        &ConfigOverrides::default(),
    )
    .unwrap();

    if config.admin.token.as_deref() == Some("legacy-token") {
        assert!(config.admin.auth.providers.is_empty());
        assert!(
            warnings.iter().any(|warning| {
                warning.contains("static-token/legacy")
                    && warning.contains(ADMIN_AUTH_PROVIDERS_JSON_ENV)
            }),
            "{warnings:?}"
        );
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::legacy_admin_token_is_automatically_migrated_in_memory")
        .env_remove(ADMIN_AUTH_PROVIDERS_JSON_ENV)
        .env(DEFAULT_ADMIN_TOKEN_ENV, "legacy-token")
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

fn assert_child_succeeded(output: std::process::Output) {
    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
