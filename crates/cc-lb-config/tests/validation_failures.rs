mod common;

use std::env;
use std::ffi::OsString;

use cc_lb_config::{Config, StorageConfig};

struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = env::var_os(key);
        // SAFETY: test-only; single-threaded test runner, no concurrent env access
        unsafe { env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            // SAFETY: test-only; single-threaded test runner, no concurrent env access
            unsafe { env::set_var(self.key, previous) };
        } else {
            // SAFETY: test-only; single-threaded test runner, no concurrent env access
            unsafe { env::remove_var(self.key) };
        }
    }
}

#[test]
fn missing_tls_files_fail_with_field_path() {
    let (_dir, path) = common::temp_config(
        r#"[tls]
cert_path = "/definitely/missing/cert.pem"
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(error.contains("tls.cert_path: file not found"), "{error}");
}

#[test]
fn encrypted_storage_retains_master_key_env_name() {
    let _guard = EnvGuard::set("CC_LB_TEST_MASTER_KEY_INVALID", "not-a-32-byte-hex-key");
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"[storage]
redb_path = "{}"
oauth_aead_key_env = "CC_LB_TEST_MASTER_KEY_INVALID"
"#,
            common::toml_path(&dir.path().join("credentials.redb"))
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();

    assert_eq!(config.aead.key_env, "CC_LB_TEST_MASTER_KEY_INVALID");
    assert_eq!(
        config.storage,
        StorageConfig::Redb {
            path: dir.path().join("credentials.redb")
        }
    );
}

#[test]
fn validation_failures_none_mode_legacy_field() {
    let (_dir, path) = common::temp_config(
        r#"[none_mode]
upstream_credential_ref = "x"

[api_keys]
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(
        error.contains("none_mode.upstream_credential_ref"),
        "{error}"
    );
    assert!(error.contains("principal.allowed_upstreams"), "{error}");
}

#[test]
fn none_mode_loads_without_upstream_credential_ref() {
    let (_dir, path) = common::temp_config(
        r#"[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "anon"
upstream_kind = "anthropic_key"

[api_keys]
"#,
    );

    let config = Config::load(&path).unwrap();

    assert!(config.downstream_auth.none_mode.is_some());
}
