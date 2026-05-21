#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use tempfile::{tempdir, TempDir};

pub fn temp_config(contents: &str) -> (TempDir, PathBuf) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.toml");
    fs::write(&path, contents).unwrap();
    (dir, path)
}

pub fn write_temp_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).unwrap();
    path
}

pub fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}
