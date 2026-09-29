use std::fs;
use std::path::{Path, PathBuf};

use tempfile::{TempDir, tempdir};

pub fn temp_config(contents: &str) -> (TempDir, PathBuf) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, contents).unwrap();
    (dir, path)
}

pub fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}
