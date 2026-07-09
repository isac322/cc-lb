use std::path::PathBuf;

pub(crate) fn storage_path_from_env() -> PathBuf {
    match std::env::var_os("CC_LB_STORAGE_PATH") {
        Some(path) => PathBuf::from(path),
        None => default_storage_path(),
    }
}

fn default_storage_path() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("cc-lb")
            .join("storage.sqlite"),
        None => PathBuf::from("~/.local/share/cc-lb/storage.sqlite"),
    }
}
