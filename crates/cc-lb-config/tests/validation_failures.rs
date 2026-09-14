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
    let (_dir, path) = crate::common::temp_config(
        r#"[listener.tls]
cert_path = "/definitely/missing/cert.pem"
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(
        error.contains("listener.tls.cert_path: file not found"),
        "{error}"
    );
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
kind = "sqlite"
path = "{}"

[aead]
key_env = "CC_LB_TEST_MASTER_KEY_INVALID"
            "#,
            crate::common::toml_path(&dir.path().join("credentials.sqlite"))
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();

    assert_eq!(config.aead.key_env, "CC_LB_TEST_MASTER_KEY_INVALID");
    assert_eq!(
        config.storage,
        StorageConfig::Sqlite {
            path: dir.path().join("credentials.sqlite")
        }
    );
}

#[test]
fn zero_upstream_affinity_ttl_fails_with_field_path() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[listener]

[upstream_affinity]
ttl_days = 0
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

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
fn admin_auth_cloudflare_access_requires_audience() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[[admin.auth.providers]]
kind = "cloudflare_access"
id = "cf"
team_domain = "https://team.cloudflareaccess.com"
audiences = []
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(
        error.contains("admin.auth.providers[0].audiences"),
        "{error}"
    );
}

#[test]
fn admin_auth_cloudflare_access_requires_valid_header_name() {
    let (_dir, path) = crate::common::temp_config(
        r#"
[[admin.auth.providers]]
kind = "cloudflare_access"
id = "cf"
team_domain = "https://team.cloudflareaccess.com"
audiences = ["admin"]
header = "Cf Access Jwt"
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(error.contains("admin.auth.providers[0].header"), "{error}");
}
