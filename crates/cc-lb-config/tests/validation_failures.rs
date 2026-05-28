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
fn dangling_credentials_ref_fails_with_field_path() {
    let (_dir, path) = common::temp_config(
        r#"[upstreams.anthropic]
kind = "anthropic_direct"
auth_strategy = "api_key"
credentials_ref = "missing"
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(
        error.contains("upstreams.anthropic.credentials_ref: dangling credentials_ref"),
        "{error}"
    );
}

#[test]
fn missing_router_plugin_wasm_path_fails_with_field_path() {
    let (_dir, path) = common::temp_config(
        r#"[plugins.router_plugin]
name = "router"
"#,
    );

    let error = Config::load(&path).unwrap_err().to_string();

    assert!(
        error.contains("plugins.router_plugin.wasm_path: missing plugin wasm path"),
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
fn validation_principal_invalid_wasm_path_rejected() {
    let (dir, config_path) = common::temp_config("");
    let config_toml = format!(
        r#"[principals.alice]
allowed_models = []

[principals.alice.router_plugin]
name = "alice-router"
wasm_path = "{}/definitely-missing.wasm"
"#,
        common::toml_path(dir.path()),
    );
    std::fs::write(&config_path, config_toml).unwrap();

    let error = Config::load(&config_path).unwrap_err().to_string();

    assert!(
        error.contains("principals.alice.router_plugin.wasm_path"),
        "error must name the per-principal field path, got: {error}"
    );
    assert!(
        error.contains("file not found"),
        "error must surface the file-not-found cause, got: {error}"
    );
}

#[test]
fn validation_principal_empty_plugin_name_rejected() {
    let (dir, config_path) = common::temp_config("");
    let wasm = common::write_temp_file(dir.path(), "router.wasm", b"\0asm\x01\x00\x00\x00");
    let config_toml = format!(
        r#"[principals.alice]
allowed_models = []

[principals.alice.router_plugin]
name = ""
wasm_path = "{}"
"#,
        common::toml_path(&wasm),
    );
    std::fs::write(&config_path, config_toml).unwrap();

    let error = Config::load(&config_path).unwrap_err().to_string();

    assert!(
        error.contains("principals.alice.router_plugin.name"),
        "error must name the per-principal field path, got: {error}"
    );
    assert!(
        error.contains("plugin name is required"),
        "error must surface the empty-name cause, got: {error}"
    );
}

#[test]
fn validation_principal_duplicate_observability_hook_names_rejected() {
    let (dir, config_path) = common::temp_config("");
    let wasm = common::write_temp_file(dir.path(), "hook.wasm", b"\0asm\x01\x00\x00\x00");
    let wasm_path = common::toml_path(&wasm);
    let config_toml = format!(
        r#"[principals.alice]
allowed_models = []

[[principals.alice.observability_hooks]]
name = "shared-hook"
wasm_path = "{wasm_path}"

[[principals.alice.observability_hooks]]
name = "shared-hook"
wasm_path = "{wasm_path}"
"#,
    );
    std::fs::write(&config_path, config_toml).unwrap();

    let error = Config::load(&config_path).unwrap_err().to_string();

    assert!(
        error.contains("principals.alice.observability_hooks"),
        "error must name the principal hooks field path, got: {error}"
    );
    assert!(
        error.contains("duplicate plugin name") && error.contains("shared-hook"),
        "error must surface the duplicate-name cause + the offending name, got: {error}"
    );
}

#[test]
fn validation_same_plugin_name_across_principals_with_different_config_accepted() {
    let (dir, config_path) = common::temp_config("");
    let wasm_a = common::write_temp_file(dir.path(), "router-a.wasm", b"\0asm\x01\x00\x00\x00");
    let wasm_b = common::write_temp_file(dir.path(), "router-b.wasm", b"\0asm\x01\x00\x00\x00");
    let config_toml = format!(
        r#"[principals.alice]
allowed_models = []

[principals.alice.router_plugin]
name = "shared-router"
wasm_path = "{wasm_a}"

[principals.alice.router_plugin.config]
upstream = "alice-upstream"

[principals.bob]
allowed_models = []

[principals.bob.router_plugin]
name = "shared-router"
wasm_path = "{wasm_b}"

[principals.bob.router_plugin.config]
upstream = "bob-upstream"
"#,
        wasm_a = common::toml_path(&wasm_a),
        wasm_b = common::toml_path(&wasm_b),
    );
    std::fs::write(&config_path, config_toml).unwrap();

    let config = Config::load(&config_path).expect(
        "same plugin name across different principals must be accepted (per-principal slot keys)",
    );
    let alice_router = config
        .principals
        .get("alice")
        .and_then(|p| p.router_plugin.as_ref())
        .expect("alice router_plugin present");
    let bob_router = config
        .principals
        .get("bob")
        .and_then(|p| p.router_plugin.as_ref())
        .expect("bob router_plugin present");
    assert_eq!(alice_router.name, "shared-router");
    assert_eq!(bob_router.name, "shared-router");
    assert_ne!(
        alice_router.config, bob_router.config,
        "the two principals must hold distinct PluginRef.config values"
    );
}
