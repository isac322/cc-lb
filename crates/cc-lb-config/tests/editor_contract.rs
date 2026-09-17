use std::process::Command;

use cc_lb_config::{Config, STORAGE_URL_REDACTION_SENTINEL};
use serde_json::json;

const PROVENANCE_CHILD: &str = "CC_LB_CONFIG_PROVENANCE_CHILD";

#[test]
fn partial_toml_parses_without_materializing_defaults() {
    let value = Config::partial_toml_json(
        "[listener]\nadmin_addr = \"127.0.0.1:9191\"\n\n[storage]\nkind = \"sqlite\"\npath = \"./local.sqlite\"\n",
    )
    .unwrap();

    assert_eq!(value["listener"]["admin_addr"], "127.0.0.1:9191");
    assert_eq!(value["storage"]["kind"], "sqlite");
    assert!(value.get("timeouts").is_none());
}

#[test]
fn partial_json_serialization_treats_null_as_unset() {
    let toml = Config::partial_json_toml(json!({
        "listener": {
            "admin_addr": null,
            "metrics_addr": "127.0.0.1:9192",
        },
        "oauth": { "anthropic": null },
    }))
    .unwrap();
    let parsed = Config::partial_toml_json(&toml).unwrap();

    assert!(parsed["listener"].get("admin_addr").is_none());
    assert_eq!(parsed["listener"]["metrics_addr"], "127.0.0.1:9192");
    assert!(parsed["oauth"].get("anthropic").is_none());
}

#[test]
fn redaction_sentinel_is_not_a_valid_postgres_url() {
    assert!(cc_lb_config::validate_postgres_url(STORAGE_URL_REDACTION_SENTINEL).is_err());
}

#[test]
fn structural_validation_does_not_access_tls_or_sqlite_paths() {
    let dir = tempfile::tempdir().unwrap();
    let config: Config = toml::from_str(&format!(
        r#"[listener.tls]
cert_path = "{}"
key_path = "{}"

[storage]
kind = "sqlite"
path = "{}"
"#,
        crate::common::toml_path(&dir.path().join("missing-cert.pem")),
        crate::common::toml_path(&dir.path().join("missing-key.pem")),
        crate::common::toml_path(&dir.path().join("missing").join("storage.sqlite")),
    ))
    .unwrap();

    config.validate_without_filesystem().unwrap();
}

#[test]
fn structural_validation_requires_tls_paths_without_reading_them() {
    let config: Config = toml::from_str(
        r#"[listener.tls]
cert_path = "/definitely/missing/cert.pem"
"#,
    )
    .unwrap();

    let error = config
        .validate_without_filesystem()
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("listener.tls.key_path: missing TLS key path"),
        "{error}"
    );
}

#[test]
fn environment_provenance_is_schema_filtered_and_excludes_secret_payload_envs() {
    if std::env::var_os(PROVENANCE_CHILD).is_some() {
        let overrides = Config::environment_overrides();
        assert!(overrides.iter().any(|entry| {
            entry.path == "upstream_affinity.ttl_days"
                && entry.name == "CC_LB_UPSTREAM_AFFINITY__TTL_DAYS"
                && !entry.sensitive
        }));
        assert!(overrides.iter().any(|entry| {
            entry.path == "storage.url" && entry.name == "CC_LB_STORAGE__URL" && entry.sensitive
        }));
        for secret in [
            "CC_LB_ADMIN_TOKEN",
            "CC_LB_MASTER_KEY",
            "CC_LB_CLUSTER_TOKEN",
        ] {
            assert!(!overrides.iter().any(|entry| entry.name == secret));
        }
        assert!(
            !overrides
                .iter()
                .any(|entry| entry.name == "CC_LB_NOT_A_CONFIG_FIELD")
        );
        return;
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("editor_contract::environment_provenance_is_schema_filtered_and_excludes_secret_payload_envs")
        .env(PROVENANCE_CHILD, "1")
        .env("CC_LB_UPSTREAM_AFFINITY__TTL_DAYS", "30")
        .env("CC_LB_STORAGE__URL", "postgres://secret@localhost/db")
        .env("CC_LB_ADMIN_TOKEN", "secret-admin-token")
        .env("CC_LB_MASTER_KEY", "secret-master-key")
        .env("CC_LB_CLUSTER_TOKEN", "secret-cluster-token")
        .env("CC_LB_NOT_A_CONFIG_FIELD", "ignored")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
