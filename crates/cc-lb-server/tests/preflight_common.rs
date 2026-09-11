#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use cc_lb_config::{Config, StorageConfig};
pub fn base_config() -> Config {
    Config::default()
}

pub fn use_temp_sqlite(config: &mut Config, prefix: &str) {
    config.storage = StorageConfig::Sqlite {
        path: unique_sqlite_path(prefix),
    };
}

fn unique_sqlite_path(prefix: &str) -> std::path::PathBuf {
    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{}-{sequence}.sqlite", std::process::id()))
}
