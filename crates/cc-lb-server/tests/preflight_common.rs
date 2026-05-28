#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;

use cc_lb_config::{AuthStrategy, Config, StorageConfig, UpstreamKind, UpstreamSpec};
use url::Url;

pub struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    pub fn remove(key: &'static str) -> Self {
        let previous = env::var_os(key);
        unsafe { env::remove_var(key) };
        Self { key, previous }
    }

    pub fn set(key: &'static str, value: &str) -> Self {
        let previous = env::var_os(key);
        unsafe { env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            unsafe { env::set_var(self.key, previous) };
        } else {
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

pub fn use_temp_redb(config: &mut Config, prefix: &str, key_env: &'static str) {
    config.storage = StorageConfig::Redb {
        path: unique_redb_path(prefix),
    };
    config.aead.key_env = key_env.to_owned();
}

fn unique_redb_path(prefix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.redb", std::process::id()))
}
