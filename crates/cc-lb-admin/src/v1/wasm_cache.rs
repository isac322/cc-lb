use std::path::PathBuf;

use crate::AdminState;

pub(crate) fn data_dir(state: &AdminState) -> PathBuf {
    if let Ok(path) = std::env::var("CC_LB_DATA_DIR") {
        return PathBuf::from(path);
    }
    state
        .config
        .runtime
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./data"))
}

pub(crate) fn wasm_cache_path(state: &AdminState, sha256_hex: &str) -> PathBuf {
    data_dir(state)
        .join("plugins")
        .join("wasm")
        .join("cache")
        .join(format!("{sha256_hex}.wasm"))
}
