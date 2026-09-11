use std::path::PathBuf;

use crate::AdminState;

pub(crate) fn data_dir(state: &AdminState) -> PathBuf {
    let configured = state.config.current_config().runtime.data_dir.clone();
    data_dir_with_lookup(configured, crate::read_env_utf8)
}

fn data_dir_with_lookup(
    configured: Option<PathBuf>,
    lookup: impl FnOnce(&str) -> Result<String, std::env::VarError>,
) -> PathBuf {
    if let Ok(path) = lookup("CC_LB_DATA_DIR") {
        return PathBuf::from(path);
    }
    configured.unwrap_or_else(|| PathBuf::from("./data"))
}

pub(crate) fn wasm_cache_path(state: &AdminState, sha256_hex: &str) -> PathBuf {
    wasm_cache_path_from_data_dir(data_dir(state), sha256_hex)
}

fn wasm_cache_path_from_data_dir(data_dir: PathBuf, sha256_hex: &str) -> PathBuf {
    data_dir
        .join("plugins")
        .join("wasm")
        .join("cache")
        .join(format!("{sha256_hex}.wasm"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injected_data_dir_lookup_preserves_precedence_and_fallbacks() {
        let configured = PathBuf::from("configured-data");
        assert_eq!(
            data_dir_with_lookup(Some(configured.clone()), |name| {
                assert_eq!(name, "CC_LB_DATA_DIR");
                Ok("ambient-data".to_owned())
            }),
            PathBuf::from("ambient-data")
        );

        assert_eq!(
            data_dir_with_lookup(Some(configured.clone()), |_| {
                Err(std::env::VarError::NotPresent)
            }),
            configured
        );

        assert_eq!(
            data_dir_with_lookup(Some(PathBuf::from("configured-data")), |_| {
                Err(std::env::VarError::NotUnicode(std::ffi::OsString::from(
                    "invalid-unicode",
                )))
            }),
            PathBuf::from("configured-data")
        );

        assert_eq!(
            data_dir_with_lookup(None, |_| Err(std::env::VarError::NotPresent)),
            PathBuf::from("./data")
        );
    }

    #[test]
    fn cache_path_derivation_preserves_directory_layout() {
        let root = PathBuf::from("configured-data");
        assert_eq!(
            wasm_cache_path_from_data_dir(root.clone(), "abcdef"),
            root.join("plugins")
                .join("wasm")
                .join("cache")
                .join("abcdef.wasm")
        );
    }
}
