#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;

use cc_lb_config::{AuthStrategy, Config, PluginRef, UpstreamKind, UpstreamSpec};
use url::Url;

pub struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    pub fn remove(key: &'static str) -> Self {
        let previous = env::var_os(key);
        env::remove_var(key);
        Self { key, previous }
    }

    pub fn set(key: &'static str, value: &str) -> Self {
        let previous = env::var_os(key);
        env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            env::set_var(self.key, previous);
        } else {
            env::remove_var(self.key);
        }
    }
}

pub fn base_config() -> Config {
    Config {
        upstreams: HashMap::new(),
        ..Config::default()
    }
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
