use std::process::Command;

use cc_lb_config::{ADMIN_AUTH_PROVIDERS_JSON_ENV, AdminAuthProviderConfig, Config};

const ENV_OVERRIDE_CHILD: &str = "CC_LB_CONFIG_ENV_OVERRIDE_CHILD";
const ADMIN_AUTH_ENV_CHILD: &str = "CC_LB_CONFIG_ADMIN_AUTH_ENV_CHILD";
const ADMIN_AUTH_INVALID_CHILD: &str = "CC_LB_CONFIG_ADMIN_AUTH_INVALID_CHILD";
const POSTGRES_POOL_ENV_CHILD: &str = "CC_LB_CONFIG_POSTGRES_POOL_ENV_CHILD";

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

#[test]
fn postgres_tls_environment_overrides_preserve_url_and_pool_intent() {
    if let Ok(scenario) = std::env::var(POSTGRES_POOL_ENV_CHILD) {
        assert_postgres_tls_environment_overrides(&scenario);
        return;
    }

    for scenario in ["url_only", "explicit_pools", "independent_pools"] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child.arg("--exact").arg(
            "load_env_override::postgres_tls_environment_overrides_preserve_url_and_pool_intent",
        );
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CC_LB_") {
                child.env_remove(name);
            }
        }
        child.env(POSTGRES_POOL_ENV_CHILD, scenario).env(
            "CC_LB_STORAGE__URL",
            "postgres://env-host/env-db?sslmode=verify-full",
        );
        if scenario == "explicit_pools" {
            child
                .env("CC_LB_STORAGE__POOL__SSLMODE", "prefer")
                .env("CC_LB_SCHEDULER__SEPARATE_POOL__SSLMODE", "disable");
        }
        assert_child_succeeded(child.output().unwrap());
    }
}

fn assert_postgres_tls_environment_overrides(scenario: &str) {
    let (storage_sslmode, scheduler_sslmode, expected_storage, expected_scheduler) = match scenario
    {
        "url_only" => ("", "", None, None),
        "explicit_pools" => (
            "sslmode = \"disable\"",
            "sslmode = \"require\"",
            Some("prefer"),
            Some("disable"),
        ),
        "independent_pools" => ("sslmode = \"prefer\"", "", Some("prefer"), None),
        _ => panic!("unknown postgres TLS environment scenario"),
    };
    let raw_toml = format!(
        r#"[storage]
kind = "postgres"
url = "postgres://toml-host/toml-db?sslmode=require"

[storage.pool]
max_connections = 3
{storage_sslmode}

[scheduler.separate_pool]
max_connections = 4
{scheduler_sslmode}

[cluster]
instance_url = "http://127.0.0.1:9090"
"#
    );
    let (_dir, path) = crate::common::temp_config(&raw_toml);

    for config in [
        Config::load(&path).expect("file config with TLS environment overrides"),
        Config::from_stored_toml_with_env(&raw_toml, Default::default())
            .expect("stored config with TLS environment overrides"),
    ] {
        let json = serde_json::to_value(&config).unwrap();
        assert_eq!(
            json["storage"]["url"], "postgres://env-host/env-db?sslmode=verify-full",
            "{scenario}",
        );
        assert_eq!(json["storage"]["pool"]["max_connections"], 3);
        assert_eq!(json["scheduler"]["separate_pool"]["max_connections"], 4);
        for (pool, expected) in [
            (&json["storage"]["pool"], expected_storage),
            (&json["scheduler"]["separate_pool"], expected_scheduler),
        ] {
            match expected {
                Some(sslmode) => assert_eq!(pool["sslmode"], sslmode, "{scenario}"),
                None => assert!(
                    !pool.as_object().unwrap().contains_key("sslmode"),
                    "{scenario}: omitted pool sslmode materialized: {pool}",
                ),
            }
        }
    }
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
fn invalid_admin_auth_provider_json_is_rejected() {
    if std::env::var_os(ADMIN_AUTH_INVALID_CHILD).is_some() {
        assert_invalid_admin_auth_env_fails();
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("load_env_override::invalid_admin_auth_provider_json_is_rejected")
        .env(ADMIN_AUTH_INVALID_CHILD, "1")
        .env(ADMIN_AUTH_PROVIDERS_JSON_ENV, "not-json")
        .output()
        .unwrap();

    assert_child_succeeded(output);
}

fn assert_invalid_admin_auth_env_fails() {
    let (_dir, path) = crate::common::temp_config(
        r#"[listener]
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(error.contains(ADMIN_AUTH_PROVIDERS_JSON_ENV), "{error}");
    assert!(error.contains("invalid"), "{error}");
}

fn assert_child_succeeded(output: std::process::Output) {
    assert!(
        output.status.success(),
        "child test failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
