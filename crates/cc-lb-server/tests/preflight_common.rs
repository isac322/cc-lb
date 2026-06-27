#![allow(dead_code)]

use std::env;
use std::ffi::OsString;

use cc_lb_config::{Config, StorageConfig};

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
    Config::default()
}

pub fn use_temp_sqlite(config: &mut Config, prefix: &str, key_env: &'static str) {
    config.storage = StorageConfig::Sqlite {
        path: unique_sqlite_path(prefix),
    };
    config.aead.key_env = key_env.to_owned();
}

fn unique_sqlite_path(prefix: &str) -> std::path::PathBuf {
    let nanos = cc_lb_core::Clock::now(&cc_lb_core::SystemClock)
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}.sqlite", std::process::id()))
}
