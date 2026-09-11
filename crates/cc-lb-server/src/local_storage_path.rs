use std::path::PathBuf;

pub(crate) fn storage_path_from_env() -> PathBuf {
    storage_path_from_sources(
        std::env::var_os("CC_LB_STORAGE_PATH"),
        std::env::var_os("HOME"),
    )
}

fn storage_path_from_sources(
    configured_path: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> PathBuf {
    if let Some(path) = configured_path {
        return PathBuf::from(path);
    }
    match home {
        Some(home) => PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("cc-lb")
            .join("storage.sqlite"),
        None => PathBuf::from("~/.local/share/cc-lb/storage.sqlite"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_path_source_precedence_and_fallbacks() {
        assert_eq!(
            storage_path_from_sources(
                Some(std::ffi::OsString::from("/explicit/storage.sqlite")),
                Some(std::ffi::OsString::from("/home/ignored")),
            ),
            PathBuf::from("/explicit/storage.sqlite"),
        );
        assert_eq!(
            storage_path_from_sources(None, Some(std::ffi::OsString::from("/home/test"))),
            PathBuf::from("/home/test/.local/share/cc-lb/storage.sqlite"),
        );
        assert_eq!(
            storage_path_from_sources(None, None),
            PathBuf::from("~/.local/share/cc-lb/storage.sqlite"),
        );
    }
}
