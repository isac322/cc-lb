#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;

use cc_lb_config::{AuthStrategy, Config, PluginRef, StorageConfig, UpstreamKind, UpstreamSpec};
use url::Url;

pub struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    pub fn remove(key: &'static str) -> Self {
        let previous = env::var_os(key);
        // SAFETY: test-only; single-threaded test runner, no concurrent env access
        unsafe { env::remove_var(key) };
        Self { key, previous }
    }

    pub fn set(key: &'static str, value: &str) -> Self {
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

pub fn base_config() -> Config {
    Config {
        upstreams: HashMap::new(),
        ..Config::default()
    }
}

pub fn use_temp_redb(config: &mut Config, name: &str, key_env: &str) {
    config.storage = StorageConfig::Redb {
        path: std::env::temp_dir().join(format!("cc-lb-{name}-{}.redb", std::process::id())),
    };
    config.aead.key_env = key_env.to_owned();
}

pub fn authn_plugin(name: &str, wasm_path: impl Into<std::path::PathBuf>) -> PluginRef {
    PluginRef {
        name: name.to_owned(),
        wasm_path: Some(wasm_path.into()),
        ..PluginRef::default()
    }
}

pub fn upstream(name: &str, kind: UpstreamKind, base_url: Option<&str>) -> (String, UpstreamSpec) {
    (
        name.to_owned(),
        UpstreamSpec {
            kind,
            base_url: base_url.map(|value| Url::parse(value).unwrap()),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    )
}
